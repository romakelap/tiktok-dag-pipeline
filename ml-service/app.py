import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, Field


# ============================================================
# Basic path and environment setup
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = ROOT_DIR / "artifacts"

ENV_FILE = Path(os.getenv("ML_ENV_FILE", ROOT_DIR / ".env"))
load_dotenv(ENV_FILE, override=True)

MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0")
ENABLE_NLP_SUMMARY = os.getenv("ENABLE_NLP_SUMMARY", "true").lower() == "true"


# ============================================================
# Artifact paths
# ============================================================

RF_MODEL_PATH = ARTIFACT_DIR / "rf_viral.pkl"
SVM_MODEL_PATH = ARTIFACT_DIR / "svm_engagement.pkl"
KMEANS_MODEL_PATH = ARTIFACT_DIR / "kmeans_content.pkl"
CATEGORY_CLASSIFIER_PATH = ARTIFACT_DIR / "category_classifier.pkl"
FEATURE_CONFIG_PATH = ARTIFACT_DIR / "feature_columns.json"
NLP_MODEL_PATH = ARTIFACT_DIR / "nlp_summary"
LSTM_MODEL_PATH = ARTIFACT_DIR / "lstm_forecaster.pt"
LSTM_METADATA_PATH = ARTIFACT_DIR / "lstm_scalers.pkl"

import torch
import torch.nn as nn
import pickle

class LSTMForecaster(nn.Module):
    def __init__(self, input_size=4, hidden_size=64, num_layers=2, output_size=4, forecast_horizon=7):
        super(LSTMForecaster, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.forecast_horizon = forecast_horizon
        self.output_size = output_size
        
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, forecast_horizon * output_size)
        
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = out[:, -1, :] # last time step
        out = self.fc(out)  # shape: (batch, forecast_horizon * output_size)
        return out



# ============================================================
# Default feature columns aligned with vw_ml_feature_store
# ============================================================

DEFAULT_NUMERIC_FEATURES = [
    "follower_count_num",
    "duration_seconds",
    "title_length",
    "hashtag_count",
    "mention_count",
    "emoji_count",
    "has_question",
    "published_day_of_week",
    "published_hour",
    "linked_hashtag_count",
    "avg_hashtag_views",
    "avg_hashtag_video_count",
    "avg_hashtag_engagement_rate",
    "avg_hashtag_competition_score",
    "duration_bucket_encoded",
    "follower_tier_encoded",
    "is_ai_video",
    "is_promote",
    "is_latest",
    "total_products_linked",
    "monetization_score",
    "revenue_tier_encoded",
]

DEFAULT_TEXT_FEATURE = "nlp_text"


# ============================================================
# FastAPI initialization
# ============================================================

app = FastAPI(
    title="TikTok BI ML Service",
    description="ML REST API for viral prediction, engagement classification, content clustering, summarization, and hashtag recommendation.",
    version=MODEL_VERSION,
)


# ============================================================
# Request / Response Schemas
# ============================================================

class FeatureRequest(BaseModel):
    features: Dict[str, Any] = Field(
        ...,
        description="Single video/account feature dictionary aligned with vw_ml_feature_store columns.",
    )


class BatchFeatureRequest(BaseModel):
    items: List[Dict[str, Any]] = Field(
        ...,
        description="Batch list of feature dictionaries.",
    )


class ForecastRequest(BaseModel):
    history: List[List[float]] = Field(
        ...,
        description="14 days of historical metrics, each row containing [views, likes, comments, shares]"
    )



class SummaryVideoItem(BaseModel):
    title: Optional[str] = ""
    caption: Optional[str] = ""
    hashtags: Optional[List[str]] = []
    views: Optional[int] = 0
    engagement_rate: Optional[float] = 0.0
    published_at: Optional[str] = None


class SummaryRequest(BaseModel):
    account_name: Optional[str] = ""
    period: Optional[str] = "weekly"
    videos: List[SummaryVideoItem]


class HashtagCandidate(BaseModel):
    tag_title: str
    views_count_num: Optional[float] = 0
    video_count_num: Optional[float] = 0
    avg_views_per_video: Optional[float] = 0
    avg_engagement_rate: Optional[float] = 0
    competition_level: Optional[str] = "medium"
    trending_score: Optional[float] = 0
    revenue_efficiency: Optional[float] = 0


class HashtagRecommendRequest(BaseModel):
    account_name: Optional[str] = ""
    content_keywords: Optional[List[str]] = []
    current_hashtags: Optional[List[str]] = []
    candidate_hashtags: Optional[List[HashtagCandidate]] = []
    limit: Optional[int] = 10


# ============================================================
# Utility Functions
# ============================================================

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default

    try:
        if pd.isna(value):
            return default
    except Exception:
        pass

    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int."""
    if value is None:
        return default

    try:
        if pd.isna(value):
            return default
    except Exception:
        pass

    try:
        return int(value)
    except Exception:
        return default


def clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    """Keep score inside 0-1 range."""
    return max(minimum, min(maximum, value))


def load_feature_config() -> Dict[str, Any]:
    """
    Load feature config from artifacts/feature_columns.json.
    If missing, fallback to default feature columns.
    """
    if not FEATURE_CONFIG_PATH.exists():
        return {
            "numeric_features": DEFAULT_NUMERIC_FEATURES,
            "text_feature": DEFAULT_TEXT_FEATURE,
            "kmeans_features": DEFAULT_NUMERIC_FEATURES + [DEFAULT_TEXT_FEATURE],
        }

    with open(FEATURE_CONFIG_PATH, "r", encoding="utf-8") as file:
        config = json.load(file)

    return {
        "numeric_features": config.get("numeric_features", DEFAULT_NUMERIC_FEATURES),
        "text_feature": config.get("text_feature", DEFAULT_TEXT_FEATURE),
        "kmeans_features": config.get(
            "kmeans_features",
            DEFAULT_NUMERIC_FEATURES + [DEFAULT_TEXT_FEATURE],
        ),
    }


def prepare_numeric_frame(features: Dict[str, Any], numeric_features: List[str]) -> pd.DataFrame:
    """
    Convert request feature dict into one-row DataFrame.
    Missing numeric columns are filled with 0.
    """
    row = {}

    for column in numeric_features:
        row[column] = safe_float(features.get(column, 0))

    return pd.DataFrame([row])


def prepare_kmeans_frame(
    features: Dict[str, Any],
    kmeans_features: List[str],
    text_feature: str,
) -> pd.DataFrame:
    """
    Convert request feature dict into one-row DataFrame for KMeans.
    KMeans model expects numeric features + nlp_text.
    """
    row = {}

    for column in kmeans_features:
        if column == text_feature:
            row[column] = str(features.get(column, ""))
        else:
            row[column] = safe_float(features.get(column, 0))

    return pd.DataFrame([row])


def get_positive_probability(model: Any, x: pd.DataFrame) -> Dict[str, Any]:
    """
    Get positive class probability from Random Forest pipeline.
    Handles class labels as int or string.
    """
    prediction = model.predict(x)[0]

    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(x)[0]
        classes = list(model.classes_)

        if 1 in classes:
            positive_index = classes.index(1)
        elif "1" in classes:
            positive_index = classes.index("1")
        else:
            positive_index = int(np.argmax(probabilities))

        viral_probability = float(probabilities[positive_index])
    else:
        viral_probability = float(prediction)

    return {
        "viral_prediction": int(prediction),
        "viral_probability": round(viral_probability, 6),
    }


def get_classification_result(model: Any, x: pd.DataFrame) -> Dict[str, Any]:
    """
    Get engagement tier prediction and probability distribution from SVM.
    """
    tier = str(model.predict(x)[0])
    confidence = None
    probabilities = {}

    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(x)[0]
        classes = list(model.classes_)

        probabilities = {
            str(label): round(float(score), 6)
            for label, score in zip(classes, proba)
        }

        confidence = round(float(np.max(proba)), 6)

    return {
        "engagement_tier": tier,
        "confidence": confidence,
        "probabilities": probabilities,
    }


def calculate_hashtag_score(candidate: HashtagCandidate) -> float:
    """
    Rule-based hashtag scoring for API recommendation endpoint.
    This endpoint does not require DB access.
    """
    engagement = safe_float(candidate.avg_engagement_rate)
    views = safe_float(candidate.views_count_num)
    trending_score = safe_float(candidate.trending_score)
    revenue_efficiency = safe_float(candidate.revenue_efficiency)

    competition_level = str(candidate.competition_level or "").lower()

    competition_bonus = {
        "low": 0.18,
        "medium": 0.12,
        "high": 0.06,
        "extreme": 0.02,
    }.get(competition_level, 0.05)

    views_score = min(math.log10(views + 1) / 12, 1)
    trending_component = min(trending_score / 100, 1)
    revenue_component = min(revenue_efficiency / 100, 1)

    score = (
        engagement * 4.0
        + views_score * 0.25
        + trending_component * 0.25
        + revenue_component * 0.15
        + competition_bonus
    )

    return clamp(score)


# ============================================================
# Hugging Face Summarizer
# We use model.generate(), not pipeline("summarization"),
# because Transformers v5 may not support summarization pipeline alias.
# ============================================================

class HuggingFaceSummarizer:
    def __init__(self, model_path: Path):
        self.enabled = False
        self.tokenizer = None
        self.model = None
        self.torch = None
        self.error_message = None

        if not ENABLE_NLP_SUMMARY:
            self.error_message = "NLP summary disabled by ENABLE_NLP_SUMMARY=false"
            return

        if not model_path.exists():
            self.error_message = f"NLP model path not found: {model_path}"
            return

        try:
            import torch
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

            self.torch = torch
            self.tokenizer = AutoTokenizer.from_pretrained(model_path)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
            self.model.eval()
            self.enabled = True

        except Exception as error:
            self.error_message = str(error)
            self.enabled = False

    def summarize(self, text: str, max_length: int = 120, min_length: int = 20) -> str:
        """
        Generate summary using Hugging Face seq2seq model.
        Return fallback text if model is disabled or text too short.
        """
        clean_text = str(text or "").strip()

        if not clean_text:
            return ""

        if not self.enabled:
            return self.fallback_summary(clean_text)

        if len(clean_text) < 50:
            return clean_text

        try:
            inputs = self.tokenizer(
                clean_text,
                return_tensors="pt",
                truncation=True,
                max_length=512,
            )

            with self.torch.no_grad():
                output_ids = self.model.generate(
                    **inputs,
                    max_length=max_length,
                    min_length=min_length,
                    num_beams=4,
                    do_sample=False,
                    early_stopping=True,
                )

            return self.tokenizer.decode(
                output_ids[0],
                skip_special_tokens=True,
            )

        except Exception:
            return self.fallback_summary(clean_text)

    @staticmethod
    def fallback_summary(text: str) -> str:
        """
        Simple fallback summary if Hugging Face is unavailable.
        """
        sentences = text.split(".")
        selected = [sentence.strip() for sentence in sentences if sentence.strip()]

        return ". ".join(selected[:3]) + "." if selected else text[:300]


# ============================================================
# ML Artifact Loader
# Models are loaded once when service starts.
# ============================================================

class MLRuntime:
    def __init__(self):
        self.feature_config = load_feature_config()

        self.numeric_features = self.feature_config["numeric_features"]
        self.text_feature = self.feature_config["text_feature"]
        self.kmeans_features = self.feature_config["kmeans_features"]

        self.rf_model = self.load_model(RF_MODEL_PATH)
        self.svm_model = self.load_model(SVM_MODEL_PATH)
        self.kmeans_model = self.load_model(KMEANS_MODEL_PATH)
        self.category_classifier = self.load_model(CATEGORY_CLASSIFIER_PATH)

        self.summarizer = HuggingFaceSummarizer(NLP_MODEL_PATH)

        # Load LSTM
        self.lstm_model = None
        self.lstm_meta = None
        try:
            if LSTM_MODEL_PATH.exists() and LSTM_METADATA_PATH.exists():
                with open(LSTM_METADATA_PATH, "rb") as f:
                    self.lstm_meta = pickle.load(f)
                self.lstm_model = LSTMForecaster(
                    input_size=self.lstm_meta.get("input_size", 4),
                    hidden_size=64,
                    num_layers=2,
                    output_size=self.lstm_meta.get("output_size", 4),
                    forecast_horizon=self.lstm_meta.get("forecast_horizon", 7)
                )
                self.lstm_model.load_state_dict(torch.load(LSTM_MODEL_PATH, map_location="cpu"))
                self.lstm_model.eval()
                print("[MLRuntime] LSTM model loaded successfully.")
        except Exception as e:
            print(f"[MLRuntime WARNING] Failed to load LSTM model: {e}")

    @staticmethod
    def load_model(path: Path) -> Any:
        if not path.exists():
            return None

        try:
            return joblib.load(path)
        except Exception:
            return None

    def status(self) -> Dict[str, Any]:
        return {
            "model_version": MODEL_VERSION,
            "artifact_dir": str(ARTIFACT_DIR),
            "rf_loaded": self.rf_model is not None,
            "svm_loaded": self.svm_model is not None,
            "kmeans_loaded": self.kmeans_model is not None,
            "category_classifier_loaded": self.category_classifier is not None,
            "lstm_loaded": self.lstm_model is not None,
            "nlp_enabled": self.summarizer.enabled,
            "nlp_error": self.summarizer.error_message,
            "numeric_feature_count": len(self.numeric_features),
            "kmeans_feature_count": len(self.kmeans_features),
        }


runtime = MLRuntime()


# ============================================================
# API Endpoints
# ============================================================

@app.get("/ml/health")
def health_check():
    """
    Health check endpoint.
    Used by Postman, Spring Boot, or deployment monitoring.
    """
    return {
        "success": True,
        "message": "ML service is running",
        "data": runtime.status(),
    }


@app.post("/ml/predict/viral")
def predict_viral(request: FeatureRequest):
    """
    Predict viral probability using Random Forest artifact.
    """
    if runtime.rf_model is None:
        return {
            "success": False,
            "message": "Random Forest model is not loaded",
            "data": None,
        }

    x = prepare_numeric_frame(
        request.features,
        runtime.numeric_features,
    )

    result = get_positive_probability(runtime.rf_model, x)

    return {
        "success": True,
        "message": "Viral prediction completed",
        "data": {
            **result,
            "model": "rf_viral_prediction",
            "model_version": MODEL_VERSION,
        },
    }


@app.post("/ml/predict/engagement")
def predict_engagement(request: FeatureRequest):
    """
    Predict engagement tier using SVM artifact.
    """
    if runtime.svm_model is None:
        return {
            "success": False,
            "message": "SVM model is not loaded",
            "data": None,
        }

    x = prepare_numeric_frame(
        request.features,
        runtime.numeric_features,
    )

    result = get_classification_result(runtime.svm_model, x)

    return {
        "success": True,
        "message": "Engagement prediction completed",
        "data": {
            **result,
            "model": "svm_engagement_tier",
            "model_version": MODEL_VERSION,
        },
    }


@app.post("/ml/predict/forecast")
def predict_forecast(request: ForecastRequest):
    """
    Forecast daily engagement metrics for the next 7 days using LSTM.
    """
    if runtime.lstm_model is None:
        return {
            "success": False,
            "message": "LSTM model is not loaded",
            "data": None,
        }

    try:
        history_arr = np.array(request.history, dtype=np.float32)

        if history_arr.shape != (14, 4):
            return {
                "success": False,
                "message": f"History must have shape (14, 4), got {history_arr.shape}",
                "data": None,
            }

        # Apply log1p scaling
        log_history = np.log1p(history_arr)

        # Convert to tensor and add batch dim: shape (1, 14, 4)
        x_tensor = torch.tensor([log_history], dtype=torch.float32)

        with torch.no_grad():
            pred_tensor = runtime.lstm_model(x_tensor)
            pred_flat = pred_tensor.numpy()[0]

        # Reshape to (7, 4)
        pred_reshaped = pred_flat.reshape(7, 4)

        # Invert scale (expm1) and ensure non-negative
        pred_metrics = np.expm1(pred_reshaped)
        pred_metrics = np.clip(pred_metrics, 0, None)

        forecast_list = []
        for day in range(7):
            forecast_list.append({
                "day": day + 1,
                "predicted_views": int(round(float(pred_metrics[day, 0]))),
                "predicted_likes": int(round(float(pred_metrics[day, 1]))),
                "predicted_comments": int(round(float(pred_metrics[day, 2]))),
                "predicted_shares": int(round(float(pred_metrics[day, 3])))
            })

        return {
            "success": True,
            "message": "Time-series forecast completed",
            "data": {
                "forecast": forecast_list,
                "model": "pytorch_lstm_forecaster",
                "model_version": MODEL_VERSION
            }
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Inference failed: {str(e)}",
            "data": None
        }



@app.post("/ml/predict/cluster")
def predict_cluster(request: FeatureRequest):
    """
    Predict content cluster using KMeans artifact.
    """
    if runtime.kmeans_model is None:
        return {
            "success": False,
            "message": "KMeans model is not loaded",
            "data": None,
        }

    x = prepare_kmeans_frame(
        request.features,
        runtime.kmeans_features,
        runtime.text_feature,
    )

    cluster_id = int(runtime.kmeans_model.predict(x)[0])

    return {
        "success": True,
        "message": "Content cluster prediction completed",
        "data": {
            "cluster_id": cluster_id,
            "cluster_label": f"cluster_{cluster_id}",
            "model": "kmeans_content_cluster",
            "model_version": MODEL_VERSION,
        },
    }


@app.post("/ml/summarize/weekly")
def summarize_weekly(request: SummaryRequest):
    """
    Generate weekly summary from video list.
    """
    combined_text = build_summary_input_text(request)

    summary = runtime.summarizer.summarize(
        combined_text,
        max_length=140,
        min_length=20,
    )

    return {
        "success": True,
        "message": "Weekly summary generated",
        "data": {
            "account_name": request.account_name,
            "period": "weekly",
            "summary_text": summary,
            "video_count": len(request.videos),
            "model": "hf_indonesian_summarizer",
            "model_version": MODEL_VERSION,
            "nlp_enabled": runtime.summarizer.enabled,
        },
    }


@app.post("/ml/summarize/monthly")
def summarize_monthly(request: SummaryRequest):
    """
    Generate monthly summary from video list.
    """
    combined_text = build_summary_input_text(request)

    summary = runtime.summarizer.summarize(
        combined_text,
        max_length=180,
        min_length=30,
    )

    return {
        "success": True,
        "message": "Monthly summary generated",
        "data": {
            "account_name": request.account_name,
            "period": "monthly",
            "summary_text": summary,
            "video_count": len(request.videos),
            "model": "hf_indonesian_summarizer",
            "model_version": MODEL_VERSION,
            "nlp_enabled": runtime.summarizer.enabled,
        },
    }


@app.post("/ml/recommend/hashtags")
def recommend_hashtags(request: HashtagRecommendRequest):
    """
    Recommend hashtags from candidates sent by Spring Boot/frontend.
    """
    limit = max(1, min(safe_int(request.limit, 10), 50))

    if request.candidate_hashtags:
        scored_candidates = []

        for item in request.candidate_hashtags:
            score = calculate_hashtag_score(item)

            scored_candidates.append(
                {
                    "tag_title": item.tag_title,
                    "hashtag": f"#{item.tag_title.lstrip('#')}",
                    "recommendation_score": round(score, 6),
                    "expected_reach": safe_int(item.avg_views_per_video or item.views_count_num),
                    "expected_engagement_rate": safe_float(item.avg_engagement_rate),
                    "competition_level": item.competition_level,
                    "reasoning": "Recommended based on engagement, trend, competition, and revenue-efficiency score.",
                }
            )

        recommendations = sorted(
            scored_candidates,
            key=lambda row: row["recommendation_score"],
            reverse=True,
        )[:limit]

    else:
        recommendations = build_fallback_hashtags(request, limit)

    return {
        "success": True,
        "message": "Hashtag recommendation completed",
        "data": {
            "account_name": request.account_name,
            "recommendations": recommendations,
            "model": "rule_based_hashtag_recommender",
            "model_version": MODEL_VERSION,
        },
    }


# ============================================================
# Helper for summary endpoint
# ============================================================

def build_summary_input_text(request: SummaryRequest) -> str:
    """
    Convert video list into summarizer input text.
    """
    parts = []

    for video in request.videos:
        hashtag_text = " ".join(video.hashtags or [])

        parts.append(
            f"Judul: {video.title or ''}. "
            f"Caption: {video.caption or ''}. "
            f"Hashtag: {hashtag_text}. "
            f"Views: {video.views or 0}. "
            f"Engagement rate: {video.engagement_rate or 0}."
        )

    header = (
        f"Ringkasan performa konten TikTok untuk akun {request.account_name or 'unknown'} "
        f"pada periode {request.period or 'weekly'}. "
    )

    return header + " ".join(parts)


def build_fallback_hashtags(request: HashtagRecommendRequest, limit: int) -> List[Dict[str, Any]]:
    """
    Fallback hashtag recommendation when no candidate_hashtags are provided.
    """
    base_items = []

    for keyword in request.content_keywords or []:
        clean = str(keyword).strip().replace(" ", "")
        if clean:
            base_items.append(clean)

    for hashtag in request.current_hashtags or []:
        clean = str(hashtag).strip().replace("#", "")
        if clean:
            base_items.append(clean)

    if not base_items:
        base_items = [
            "tanamanhias",
            "plantcare",
            "berkebun",
            "dekorasirumah",
            "tipsberkebun",
        ]

    unique_items = []

    for item in base_items:
        if item not in unique_items:
            unique_items.append(item)

    recommendations = []

    for index, item in enumerate(unique_items[:limit], start=1):
        score = round(max(0.3, 1.0 - (index * 0.07)), 6)

        recommendations.append(
            {
                "tag_title": item,
                "hashtag": f"#{item}",
                "recommendation_score": score,
                "expected_reach": 0,
                "expected_engagement_rate": 0,
                "competition_level": "unknown",
            }
        )

    return recommendations


# ============================================================
# Phase 11 & Phase 12: ML Category Classifier & Smarter Insight Generator
# ============================================================

class CategoryPredictRequest(BaseModel):
    title: Optional[str] = ""
    hashtags: Optional[str] = ""
    nlp_text: Optional[str] = ""


class CategoryMetricsInput(BaseModel):
    category_name: str
    total_videos: int
    total_views: int
    avg_engagement_rate: float
    viral_score: float
    top_hashtags: List[str] = []
    top_keywords: List[str] = []


class CategoryInsightRequest(BaseModel):
    category_a: CategoryMetricsInput
    category_b: Optional[CategoryMetricsInput] = None


def predict_category_with_model(model: Any, title: str, hashtags: str, nlp_text: str) -> Dict[str, Any]:
    combined_text = f"{title} {hashtags} {nlp_text}".strip()
    category = str(model.predict([combined_text])[0])

    confidence = 0.85  # default ML confidence
    try:
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba([combined_text])[0]
            classes = list(model.classes_)
            if category in classes:
                confidence = float(proba[classes.index(category)])
        elif hasattr(model, "decision_function"):
            decision = model.decision_function([combined_text])[0]
            if len(decision.shape) > 0:
                exp_dec = np.exp(decision - np.max(decision))
                probs = exp_dec / np.sum(exp_dec)
                classes = list(model.classes_)
                if category in classes:
                    confidence = float(probs[classes.index(category)])
    except Exception as e:
        print(f"[ML WARNING] Failed to compute confidence score: {e}")

    return {
        "category": category,
        "confidence": round(confidence, 6),
    }


def generate_single_insight(cat: CategoryMetricsInput) -> str:
    er = cat.avg_engagement_rate
    if er > 0.05:
        er_desc = "sangat tinggi (High Engagement)"
    elif er > 0.02:
        er_desc = "sedang (Moderate Engagement)"
    else:
        er_desc = "perlu ditingkatkan (Low Engagement)"

    vs = cat.viral_score
    if vs > 70:
        vs_desc = "potensi viralitas luar biasa tinggi (High Viral Potential)"
    elif vs > 40:
        vs_desc = "potensi viralitas moderat (Medium Viral Potential)"
    else:
        vs_desc = "potensi viralitas yang cenderung organik dan stabil (Low Viral Potential)"

    hashtags_str = ", ".join(cat.top_hashtags[:5]) if cat.top_hashtags else "None"
    keywords_str = ", ".join(cat.top_keywords[:5]) if cat.top_keywords else "None"

    recommendations = {
        "Edukasi": (
            "Fokus pada format konten 'Micro-Learning' atau 'How-To' berdurasi singkat (30-60 detik) "
            "dengan hook visual 3 detik pertama yang kuat. Gunakan teks on-screen tebal dan transisi cepat. "
            "Buat konten berseri seperti 'Bongkar Rahasia X dalam 3 Hari' untuk meningkatkan tingkat retention rate penonton."
        ),
        "Komedi": (
            "Gunakan format sketsa singkat dengan punchline cepat, parodi dari tren audio yang sedang viral, "
            "atau konten POV (Point of View) yang relatable bagi audiens muda. Manfaatkan reaksi ekspresi wajah "
            "dan efek suara TikTok yang populer untuk memaksimalkan faktor kelucuan."
        ),
        "Kuliner": (
            "Manfaatkan elemen visual yang menggugah selera (aesthetic food shots/ASMR memasak). "
            "Buat resep praktis berdurasi 15-30 detik dengan list bahan yang jelas di caption. Konten 'review jujur' "
            "warung kaki lima tersembunyi atau tantangan kuliner unik berpotensi tinggi memicu interaksi dan share."
        ),
        "Teknologi": (
            "Fokus pada format video unboxing estetik, review gadget jujur dengan perbandingan berdampingan (side-by-side), "
            "atau tips & trik tersembunyi (hidden features) di HP/PC. Gunakan background musik lofi/synthwave "
            "dan pencahayaan neon yang futuristik."
        ),
        "Lifestyle & Home": (
            "Tampilkan konten bernuansa estetik, seperti 'A Day in My Life' vlog, dekorasi kamar minimalis, "
            "cleaning/organization ASMR, atau panduan skincare harian. Fokus pada visual warna hangat (warm tone), "
            "transisi mulus, dan suasana rumah yang nyaman (cozy vibes)."
        ),
    }

    cat_rec = recommendations.get(
        cat.category_name,
        "Buat konten yang autentik, gunakan transisi yang rapi, dan pertahankan komunikasi dua arah di kolom komentar untuk menjaga loyalitas audiens.",
    )

    insight_text = (
        f"### Analisis & Strategi Konten Kategori: **{cat.category_name}**\n\n"
        f"Kategori **{cat.category_name}** saat ini memiliki total **{cat.total_videos:,} video** dengan jangkauan kumulatif **{cat.total_views:,} views**. "
        f"Tingkat interaksi rata-rata audiens pada kategori ini berada di angka **{er*100:.2f}%** ({er_desc}), sedangkan skor viralitas berada di tingkat **{vs:.1f}/100** ({vs_desc}).\n\n"
        f"#### 🎯 Arah Strategi Konten:\n"
        f"- **Rekomendasi Format**: {cat_rec}\n"
        f"- **Eksplorasi Topik Utama**: Optimalkan kata kunci utama seperti **{keywords_str}** ke dalam skrip pembuka (hook) dan judul video.\n"
        f"- **Optimasi Hashtag**: Integrasikan kombinasi hashtag performa tinggi **{hashtags_str}** pada deskripsi video Anda untuk memudahkan algoritma menyebarkan konten ke audiens yang tepat.\n\n"
        f"#### 💡 Tips Tambahan:\n"
        f"Selalu pantau kolom komentar pada 1 jam pertama rilis konten untuk membalas pertanyaan audiens dengan kata kunci yang relevan guna mendorong performa SEO pencarian TikTok."
    )
    return insight_text


def generate_combined_insight(cat_a: CategoryMetricsInput, cat_b: CategoryMetricsInput) -> str:
    name_a = cat_a.category_name
    name_b = cat_b.category_name

    sorted_cats = sorted([name_a, name_b])
    key = f"{sorted_cats[0]} & {sorted_cats[1]}"

    combo_strategies = {
        "Edukasi & Komedi": (
            "**Strategi Edutainment (Edukasi + Komedi)**:\n"
            "Buat konten sketsa komedi pendek yang membahas fakta ilmiah menarik, sejarah unik, atau mitos populer yang salah. "
            "Contoh: Sketsa lucu berakting sebagai guru dan murid bandel yang membahas rumus matematika cepat, "
            "atau memparodikan kesalahpahaman umum sehari-hari secara edukatif. Ini sangat sukses memicu komentar dan share."
        ),
        "Edukasi & Kuliner": (
            "**Strategi Nutri-Science (Edukasi + Kuliner)**:\n"
            "Kombinasikan resep masakan dengan penjelasan ilmiah di baliknya. "
            "Contoh: Menjelaskan reaksi kimia saat memanggang kue, tips diet sehat dengan perhitungan kalori bahan makanan, "
            "atau sejarah menarik asal-usul suatu masakan tradisional sambil memperlihatkan proses memasaknya secara estetik."
        ),
        "Edukasi & Teknologi": (
            "**Strategi Tech-Tutorials (Edukasi + Teknologi)**:\n"
            "Fokus pada panduan praktis pemrograman dasar, tutorial tools AI terbaru untuk produktivitas kerja/kuliah, "
            "atau tips keamanan digital. Gunakan visual capture screen dengan teks penjelasan ringkas "
            "dan selingi dengan visual host menjelaskan analogi yang mudah dipahami orang awam."
        ),
        "Edukasi & Lifestyle & Home": (
            "**Strategi Life-Hacks & Productivity (Edukasi + Lifestyle)**:\n"
            "Sajikan konten tips mengatur rutinitas harian yang produktif, metode merapikan kamar (KonMari), "
            "atau edukasi tentang keuangan pribadi/manajemen waktu. Kemas video dengan tone warna estetik "
            "dan suara narasi voice-over yang menenangkan."
        ),
        "Komedi & Kuliner": (
            "**Strategi Fun-Mukbang & Review (Komedi + Kuliner)**:\n"
            "Lakukan review makanan dengan pembawaan humoris, reaksi lucu (react video) terhadap video resep masakan aneh/gagal, "
            "atau sketsa komedi tentang tipe-tipe pelanggan di restoran. Ini sangat memicu interaksi tinggi karena sifatnya yang menghibur."
        ),
        "Komedi & Teknologi": (
            "**Strategi IT-Humor & Parody (Komedi + Teknologi)**:\n"
            "Buat konten parodi kehidupan sehari-hari anak IT, programmer, atau pengguna gadget awam. "
            "Contoh: Parodi perbedaan ekspektasi vs realita saat membeli laptop baru, "
            "atau reaksi lucu terhadap perkembangan robot/AI yang terasa canggung. Sangat populer di kalangan komunitas tech."
        ),
        "Komedi & Lifestyle & Home": (
            "**Strategi Relatable-Vlog Parody (Komedi + Lifestyle)**:\n"
            "Tampilkan sisi tidak sempurna (relatable) dari tren kehidupan estetik. "
            "Contoh: Memparodikan rutinitas pagi 'aesthetic' yang berujung kacau karena kesiangan, "
            "atau video komedi dekorasi kamar kos low-budget yang tidak sesuai rencana. Membangun kedekatan emosional dengan penonton."
        ),
        "Kuliner & Teknologi": (
            "**Strategi Smart-Kitchen & Food-Tech (Kuliner + Teknologi)**:\n"
            "Review gadget dapur canggih dan futuristik (seperti smart oven, air fryer dengan koneksi app, dll.) "
            "sambil langsung mempraktikkannya untuk memasak. Strategi alternatif: Berbagi tips cara mengambil video makanan (food videography) yang estetik menggunakan smartphone."
        ),
        "Kuliner & Lifestyle & Home": (
            "**Strategi Cozy-Cooking & Aesthetic Vlog (Kuliner + Lifestyle)**:\n"
            "Gabungkan daily vlog aktivitas rumah tangga dengan resep masakan harian (home-cooking). "
            "Visualisasikan proses mempersiapkan bahan makanan secara perlahan (slow-paced), "
            "penataan meja makan yang estetik (table setting), dan rutinitas bersih-bersih dapur pascalapar."
        ),
        "Lifestyle & Home & Teknologi": (
            "**Strategi Smart-Home & Desk Setup (Lifestyle + Teknologi)**:\n"
            "Fokus pada dekorasi ruangan yang terintegrasi dengan gadget modern. "
            "Contoh: Memperlihatkan setup meja belajar/kerja (desk setup) minimalis dengan pencahayaan pintar (RGB sync), "
            "atau vlog merapikan rumah dengan bantuan robot vacuum cleaner dan peranti smart home otomatis."
        ),
    }

    strategy = combo_strategies.get(
        key,
        f"**Strategi Crossover Kolaboratif ({name_a} & {name_b})**:\n"
        f"Kombinasikan topik unik dari {name_a} dan visualisasi menarik dari {name_b}. "
        f"Temukan irisan audiens kedua kategori untuk menciptakan konten hibrida yang belum banyak dieksplorasi di pasar.",
    )

    avg_er = (cat_a.avg_engagement_rate + cat_b.avg_engagement_rate) / 2
    avg_vs = (cat_a.viral_score + cat_b.viral_score) / 2

    combined_keywords = sorted(list(set(cat_a.top_keywords[:3] + cat_b.top_keywords[:3])))
    combined_hashtags = sorted(list(set(cat_a.top_hashtags[:3] + cat_b.top_hashtags[:3])))
    keywords_str = ", ".join(combined_keywords)
    hashtags_str = ", ".join(combined_hashtags)

    insight_text = (
        f"### 🚀 Ide Crossover Strategis: **{name_a}** & **{name_b}**\n\n"
        f"Menggabungkan kategori **{name_a}** dan **{name_b}** memiliki potensi sinergi pasar yang luar biasa. "
        f"Kedua kategori ini menghasilkan performa interaksi gabungan rata-rata sebesar **{avg_er*100:.2f}%** ER dengan skor viralitas rata-rata **{avg_vs:.1f}/100**.\n\n"
        f"#### 🎯 Konsep Konten Crossover:\n"
        f"{strategy}\n\n"
        f"#### 💡 Panduan Optimasi Konten Gabungan:\n"
        f"- **Kata Kunci Penarik Perhatian**: Gunakan kata kunci gabungan seperti **{keywords_str}** di 3 detik pertama hook video Anda.\n"
        f"- **Kombinasi Tagging Relevan**: Tempelkan hashtag **{hashtags_str}** pada deskripsi konten Anda untuk menjaring audiens dari kedua minat kategori tersebut."
    )

    return insight_text


@app.post("/ml/predict/category")
def predict_category(request: CategoryPredictRequest):
    """
    Predict video category using the trained category_classifier.pkl.
    If the model is not loaded, it falls back to the rule-based classification.
    """
    title = request.title or ""
    hashtags = request.hashtags or ""
    nlp_text = request.nlp_text or ""

    if runtime.category_classifier is not None:
        result = predict_category_with_model(
            runtime.category_classifier,
            title,
            hashtags,
            nlp_text,
        )
        return {
            "success": True,
            "message": "Category prediction completed using ML classifier",
            "data": {
                "category": result["category"],
                "confidence_score": result["confidence"],
                "tagging_method": "ml_classifier",
                "model_version": MODEL_VERSION,
            },
        }
    else:
        # Fallback to rule-based classification
        try:
            from inference.classify_rules import classify_video
        except ImportError:
            import sys
            sys.path.append(str(Path(__file__).resolve().parent / "inference"))
            from classify_rules import classify_video

        category, confidence = classify_video(title, hashtags, nlp_text)
        return {
            "success": True,
            "message": "Category prediction completed using rule-based fallback",
            "data": {
                "category": category,
                "confidence_score": confidence,
                "tagging_method": "rule_based",
                "model_version": MODEL_VERSION,
            },
        }


@app.post("/ml/generate/category-insight")
def generate_category_insight(request: CategoryInsightRequest):
    """
    Generate professional TikTok marketing/content direction strategy in Indonesian.
    Handles single category or combined categories.
    """
    cat_a = request.category_a
    cat_b = request.category_b

    if cat_b is None:
        content_direction = generate_single_insight(cat_a)
    else:
        content_direction = generate_combined_insight(cat_a, cat_b)

    return {
        "success": True,
        "message": "Category content direction strategy generated successfully",
        "data": {
            "content_direction": content_direction,
            "model": "rule_based_insight_generator",
            "model_version": MODEL_VERSION,
        },
    }


@app.get("/ml/analytics/correlation")
def get_feature_correlation(influencer_id: Optional[int] = None):
    """
    Calculate Pearson correlation matrix between video features and engagement metrics.
    Optionally filtered by influencer_id.
    """
    try:
        from db import get_engine
        engine = get_engine()
        
        query = """
        SELECT 
            duration_seconds, 
            title_length, 
            hashtag_count, 
            mention_count, 
            emoji_count, 
            has_question, 
            views_num, 
            engagement_rate_num
        FROM vw_ml_feature_store
        """
        params = {}
        if influencer_id is not None:
            query += " WHERE influencer_id = :influencer_id"
            params["influencer_id"] = influencer_id
            
        df = pd.read_sql(query, engine, params=params)
        
        if df.empty:
            return {
                "success": True,
                "message": "No data found for correlation calculation",
                "data": {
                    "features": ["duration", "title_length", "hashtags", "mentions", "emojis", "has_question", "views", "engagement_rate"],
                    "matrix": [[0.0] * 8 for _ in range(8)]
                }
            }
            
        df = df.rename(columns={
            "duration_seconds": "duration",
            "title_length": "title_length",
            "hashtag_count": "hashtags",
            "mention_count": "mentions",
            "emoji_count": "emojis",
            "has_question": "has_question",
            "views_num": "views",
            "engagement_rate_num": "engagement_rate"
        })
        
        features = ["duration", "title_length", "hashtags", "mentions", "emojis", "has_question", "views", "engagement_rate"]
        df = df[features]
        
        corr = df.corr(method="pearson").fillna(0.0)
        matrix = [[float(val) for val in row] for row in corr.values.tolist()]
        
        return {
            "success": True,
            "message": "Correlation matrix calculated successfully",
            "data": {
                "features": features,
                "matrix": matrix
            }
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to calculate correlation matrix: {str(e)}",
            "data": None
        }


@app.get("/ml/analytics/hashtag-network")
def get_hashtag_network(influencer_id: Optional[int] = None, limit: int = 50):
    """
    Get co-occurring hashtags nodes and edge weights.
    Optionally filtered by influencer_id.
    """
    try:
        from db import get_engine
        engine = get_engine()
        
        query = """
        SELECT 
            vh1.hashtag_pk AS source_pk,
            h1.tag_title AS source_title,
            vh2.hashtag_pk AS target_pk,
            h2.tag_title AS target_title,
            COUNT(DISTINCT vh1.video_pk) AS weight
        FROM video_hashtags_echotik vh1
        INNER JOIN video_hashtags_echotik vh2 ON vh1.video_pk = vh2.video_pk AND vh1.hashtag_pk < vh2.hashtag_pk
        INNER JOIN hashtags_echotik h1 ON vh1.hashtag_pk = h1.hashtag_pk
        INNER JOIN hashtags_echotik h2 ON vh2.hashtag_pk = h2.hashtag_pk
        INNER JOIN videos_echotik v ON vh1.video_pk = v.video_pk
        """
        
        params = {}
        if influencer_id is not None:
            query += " WHERE v.influencer_id = :influencer_id"
            params["influencer_id"] = influencer_id
            
        query += """
        GROUP BY vh1.hashtag_pk, vh2.hashtag_pk, h1.tag_title, h2.tag_title
        ORDER BY weight DESC
        LIMIT :limit
        """
        params["limit"] = limit
        
        df = pd.read_sql(query, engine, params=params)
        
        nodes_dict = {}
        edges = []
        
        for _, row in df.iterrows():
            s_pk = int(row["source_pk"])
            s_title = str(row["source_title"])
            t_pk = int(row["target_pk"])
            t_title = str(row["target_title"])
            weight = int(row["weight"])
            
            if s_title not in nodes_dict:
                nodes_dict[s_title] = {"id": s_title, "label": s_title, "val": 0}
            if t_title not in nodes_dict:
                nodes_dict[t_title] = {"id": t_title, "label": t_title, "val": 0}
                
            nodes_dict[s_title]["val"] += weight
            nodes_dict[t_title]["val"] += weight
            
            edges.append({
                "source": s_title,
                "target": t_title,
                "weight": weight
            })
            
        nodes = list(nodes_dict.values())
        
        return {
            "success": True,
            "message": "Hashtag network graph generated successfully",
            "data": {
                "nodes": nodes,
                "edges": edges
            }
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to generate hashtag network graph: {str(e)}",
            "data": None
        }