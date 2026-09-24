import json
import math
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from db import get_engine  # noqa: E402


ENV_FILE = Path(os.getenv("ML_ENV_FILE", ROOT_DIR / ".env"))
load_dotenv(ENV_FILE, override=True)


ARTIFACT_DIR = ROOT_DIR / "artifacts"

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

MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0")
VALID_DAYS = int(os.getenv("VALID_DAYS", "14"))
ENABLE_NLP_SUMMARY = os.getenv("ENABLE_NLP_SUMMARY", "true").lower() == "true"

TODAY = date.today()
VALID_UNTIL = TODAY + timedelta(days=VALID_DAYS)


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

STOPWORDS = {
    "yang", "dan", "dengan", "untuk", "dari", "pada", "dalam", "atau",
    "ini", "itu", "akan", "bisa", "agar", "lebih", "kamu", "kita",
    "the", "and", "for", "with", "from", "this", "that", "into",
}


def safe_int(value, default=0):
    if value is None or pd.isna(value):
        return default

    try:
        return int(value)
    except Exception:
        return default


def safe_float(value, default=0.0):
    if value is None or pd.isna(value):
        return default

    try:
        return float(value)
    except Exception:
        return default


def safe_str(value, default=""):
    if value is None or pd.isna(value):
        return default

    return str(value)


def clamp(value, min_value=0.0, max_value=1.0):
    return max(min_value, min(max_value, value))


def to_json(value):
    return json.dumps(value, ensure_ascii=False)


def load_feature_config():
    if not FEATURE_CONFIG_PATH.exists():
        print("[CONFIG WARNING] feature_columns.json not found. Using default features.")

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


def prepare_numeric_features(df, numeric_features):
    working_df = df.copy()

    for column in numeric_features:
        if column not in working_df.columns:
            working_df[column] = 0

        working_df.loc[:, column] = pd.to_numeric(
            working_df[column],
            errors="coerce",
        ).fillna(0)

    return working_df


def extract_keywords(text_series, limit=8):
    combined_text = " ".join(
        str(item).lower()
        for item in text_series.dropna().tolist()
        if str(item).strip()
    )

    words = re.findall(r"[a-zA-Z0-9À-ÿ]+", combined_text)

    cleaned_words = [
        word
        for word in words
        if len(word) >= 4 and word not in STOPWORDS
    ]

    if not cleaned_words:
        return []

    frequency = pd.Series(cleaned_words).value_counts()

    return frequency.head(limit).index.tolist()


def extract_hashtags(hashtag_text, limit=8):
    if hashtag_text is None or pd.isna(hashtag_text):
        return []

    hashtags = re.findall(r"#[\wÀ-ÿ]+", str(hashtag_text))

    unique_hashtags = []

    for hashtag in hashtags:
        cleaned = hashtag.strip()

        if cleaned and cleaned not in unique_hashtags:
            unique_hashtags.append(cleaned)

    return unique_hashtags[:limit]


def get_day_name(day_of_week):
    mapping = {
        0: "Sunday",
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
    }

    return mapping.get(safe_int(day_of_week), "Unknown")


class HuggingFaceSummarizer:
    def __init__(self, model_path):
        self.enabled = False
        self.tokenizer = None
        self.model = None

        if not ENABLE_NLP_SUMMARY:
            print("[NLP] NLP summary disabled by ENABLE_NLP_SUMMARY=false")
            return

        if not Path(model_path).exists():
            print(f"[NLP WARNING] NLP model path not found: {model_path}")
            return

        try:
            import torch
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

            self.torch = torch
            self.tokenizer = AutoTokenizer.from_pretrained(model_path)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(model_path)
            self.model.eval()
            self.enabled = True

            print(f"[NLP] Hugging Face summarizer loaded from {model_path}")

        except Exception as error:
            print(f"[NLP WARNING] Failed to load Hugging Face summarizer: {error}")
            self.enabled = False

    def summarize(self, text, max_length=100, min_length=15):
        if not self.enabled:
            return ""

        clean_text = safe_str(text).strip()

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

        except Exception as error:
            print(f"[NLP WARNING] Summary generation failed: {error}")
            return ""


class MLArtifacts:
    def __init__(self):
        self.feature_config = load_feature_config()
        self.numeric_features = self.feature_config["numeric_features"]
        self.text_feature = self.feature_config["text_feature"]
        self.kmeans_features = self.feature_config["kmeans_features"]

        self.rf_model = self.load_model(RF_MODEL_PATH, "Random Forest")
        self.svm_model = self.load_model(SVM_MODEL_PATH, "SVM")
        self.kmeans_model = self.load_model(KMEANS_MODEL_PATH, "KMeans")
        self.category_classifier = self.load_model(CATEGORY_CLASSIFIER_PATH, "Category Classifier")

        self.summarizer = HuggingFaceSummarizer(NLP_MODEL_PATH)

        # Load LSTM model
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
                print("[MODEL] LSTM model loaded successfully.")
        except Exception as error:
            print(f"[MODEL WARNING] Failed to load LSTM model: {error}")


    @staticmethod
    def load_model(path, label):
        if not Path(path).exists():
            print(f"[MODEL WARNING] {label} artifact not found: {path}")
            return None

        try:
            model = joblib.load(path)
            print(f"[MODEL] {label} loaded: {path}")
            return model

        except Exception as error:
            print(f"[MODEL WARNING] Failed to load {label}: {error}")
            return None


def load_active_users(engine):
    sql = """
        SELECT DISTINCT user_id
        FROM tracked_accounts
        WHERE is_active = 1
        ORDER BY user_id
    """

    return pd.read_sql(sql, engine)


def load_user_features(engine, user_id):
    sql = text(
        """
        SELECT
            fs.*,
            ta.user_id,
            ta.tracking_id,
            ta.tracking_type,
            ta.nickname
        FROM vw_ml_feature_store fs
        INNER JOIN tracked_accounts ta
            ON ta.influencer_id = fs.influencer_id
        WHERE ta.user_id = :user_id
          AND ta.is_active = 1
        """
    )

    return pd.read_sql(sql, engine, params={"user_id": user_id})


def load_user_tracked_accounts(engine, user_id):
    sql = text(
        """
        SELECT
            ta.user_id,
            ta.influencer_id,
            ta.tracking_type,
            ta.nickname,
            i.unique_id,
            i.display_name
        FROM tracked_accounts ta
        INNER JOIN influencers i
            ON i.influencer_id = ta.influencer_id
        WHERE ta.user_id = :user_id
          AND ta.is_active = 1
        ORDER BY
            CASE ta.tracking_type
                WHEN 'own' THEN 1
                WHEN 'competitor' THEN 2
                WHEN 'inspiration' THEN 3
                ELSE 4
            END
        """
    )

    return pd.read_sql(sql, engine, params={"user_id": user_id})


def load_hashtag_candidates(engine, limit=50):
    sql = text(
        """
        SELECT
            h.hashtag_pk,
            h.tag_title,
            h.views_count_num,
            h.video_count_num,
            h.avg_views_per_video,
            h.avg_engagement_rate,
            h.competition_level,
            COALESCE(b.revenue_efficiency, 0) AS revenue_efficiency,
            COALESCE(b.total_gmv_usd, 0) AS total_gmv_usd,
            COALESCE(th.trending_score, 0) AS trending_score,
            COALESCE(th.is_rising, 0) AS is_rising
        FROM hashtags_echotik h
        LEFT JOIN bi_hashtag_revenue_summary b
            ON b.hashtag_pk = h.hashtag_pk
        LEFT JOIN hashtag_trending_history th
            ON th.history_id = (
                SELECT th2.history_id
                FROM hashtag_trending_history th2
                WHERE th2.hashtag_pk = h.hashtag_pk
                ORDER BY th2.snapshot_date DESC, th2.history_id DESC
                LIMIT 1
            )
        WHERE h.tag_title IS NOT NULL
        ORDER BY
            COALESCE(th.trending_score, 0) DESC,
            COALESCE(h.avg_engagement_rate, 0) DESC,
            COALESCE(h.views_count_num, 0) DESC
        LIMIT :limit
        """
    )

    return pd.read_sql(sql, engine, params={"limit": limit})


def add_ml_predictions(features, artifacts):
    df = features.copy()
    df = prepare_numeric_features(df, artifacts.numeric_features)

    text_feature = artifacts.text_feature

    if text_feature not in df.columns:
        df[text_feature] = ""

    df.loc[:, text_feature] = df[text_feature].fillna("").astype(str)

    X_numeric = df[artifacts.numeric_features]

    if artifacts.rf_model is not None:
        try:
            df.loc[:, "ml_viral_prediction"] = artifacts.rf_model.predict(X_numeric)

            if hasattr(artifacts.rf_model, "predict_proba"):
                proba = artifacts.rf_model.predict_proba(X_numeric)
                classes = list(artifacts.rf_model.classes_)

                if 1 in classes:
                    positive_index = classes.index(1)
                    df.loc[:, "ml_viral_probability"] = proba[:, positive_index]
                else:
                    df.loc[:, "ml_viral_probability"] = proba.max(axis=1)
            else:
                df.loc[:, "ml_viral_probability"] = df["ml_viral_prediction"]

        except Exception as error:
            print(f"[PREDICT WARNING] RF prediction failed: {error}")
            df.loc[:, "ml_viral_prediction"] = 0
            df.loc[:, "ml_viral_probability"] = 0.0
    else:
        df.loc[:, "ml_viral_prediction"] = df.get("label_is_viral", 0)
        df.loc[:, "ml_viral_probability"] = np.where(
            df.get("label_is_viral", 0).astype(int) == 1,
            0.75,
            0.35,
        )

    if artifacts.svm_model is not None:
        try:
            df.loc[:, "ml_engagement_tier"] = artifacts.svm_model.predict(X_numeric)

            if hasattr(artifacts.svm_model, "predict_proba"):
                df.loc[:, "ml_engagement_confidence"] = artifacts.svm_model.predict_proba(X_numeric).max(axis=1)
            else:
                df.loc[:, "ml_engagement_confidence"] = 0.65

        except Exception as error:
            print(f"[PREDICT WARNING] SVM prediction failed: {error}")
            df.loc[:, "ml_engagement_tier"] = df.get("label_engagement_tier", "low")
            df.loc[:, "ml_engagement_confidence"] = 0.50
    else:
        df.loc[:, "ml_engagement_tier"] = df.get("label_engagement_tier", "low")
        df.loc[:, "ml_engagement_confidence"] = 0.50

    if artifacts.kmeans_model is not None:
        try:
            for column in artifacts.kmeans_features:
                if column not in df.columns:
                    df[column] = "" if column == text_feature else 0

            X_kmeans = df[artifacts.kmeans_features].copy()

            if text_feature in X_kmeans.columns:
                X_kmeans.loc[:, text_feature] = X_kmeans[text_feature].fillna("").astype(str)

            df.loc[:, "ml_content_cluster"] = artifacts.kmeans_model.predict(X_kmeans)
            df.loc[:, "ml_content_cluster_label"] = df["ml_content_cluster"].apply(
                lambda value: f"cluster_{safe_int(value)}"
            )

        except Exception as error:
            print(f"[PREDICT WARNING] KMeans prediction failed: {error}")
            df.loc[:, "ml_content_cluster"] = 0
            df.loc[:, "ml_content_cluster_label"] = "cluster_0"
    else:
        df.loc[:, "ml_content_cluster"] = 0
        df.loc[:, "ml_content_cluster_label"] = "cluster_0"

    return df


def build_summary_text(group):
    top_videos = group.sort_values(
        by=["ml_viral_probability", "engagement_rate_num", "views_num"],
        ascending=[False, False, False],
    ).head(5)

    text_parts = []

    for _, row in top_videos.iterrows():
        title = safe_str(row.get("title_full")) or safe_str(row.get("title_brief"))
        hashtag_text = safe_str(row.get("hashtag_text"))
        engagement = safe_float(row.get("engagement_rate_num"))
        views = safe_int(row.get("views_num"))
        tier = safe_str(row.get("ml_engagement_tier"))

        text_parts.append(
            f"Video: {title}. Hashtag: {hashtag_text}. "
            f"Views: {views}. Engagement rate: {engagement:.4f}. Tier: {tier}."
        )

    return " ".join(text_parts)


def generate_content_recommendations(user_id, features, artifacts):
    recommendations = []

    if features.empty:
        return recommendations

    for influencer_id, group in features.groupby("influencer_id"):
        group = group.copy()

        group.loc[:, "engagement_rate_num"] = group["engagement_rate_num"].fillna(0)
        group.loc[:, "views_num"] = group["views_num"].fillna(0)
        group.loc[:, "duration_seconds"] = group["duration_seconds"].fillna(0)
        group.loc[:, "ml_viral_probability"] = group["ml_viral_probability"].fillna(0)

        top_video = group.sort_values(
            by=["ml_viral_probability", "engagement_rate_num", "views_num"],
            ascending=[False, False, False],
        ).iloc[0]

        avg_engagement = safe_float(group["engagement_rate_num"].mean())
        avg_views = safe_float(group["views_num"].mean())

        top_engagement = safe_float(top_video.get("engagement_rate_num"))
        top_views = safe_int(top_video.get("views_num"))
        viral_probability = safe_float(top_video.get("ml_viral_probability"))
        engagement_tier = safe_str(top_video.get("ml_engagement_tier"), "low")
        cluster_label = safe_str(top_video.get("ml_content_cluster_label"), "cluster_0")

        suggested_keywords = extract_keywords(group["nlp_text"], limit=8)
        suggested_hashtags = extract_hashtags(top_video.get("hashtag_text"), limit=8)

        high_performer = group[
            group["engagement_rate_num"] >= group["engagement_rate_num"].quantile(0.75)
        ]

        suggested_duration = safe_int(
            high_performer["duration_seconds"].median()
            if not high_performer.empty
            else group["duration_seconds"].median()
        )

        summary_input = build_summary_text(group)
        nlp_summary = artifacts.summarizer.summarize(summary_input) if artifacts.summarizer else ""

        if not nlp_summary:
            nlp_summary = (
                "Model ML mengidentifikasi pola konten berdasarkan performa video, "
                "engagement rate, hashtag, durasi, dan waktu posting historis."
            )

        confidence_score = clamp(
            0.50
            + min(len(group) / 100, 0.20)
            + min(viral_probability * 0.25, 0.25)
            + min(top_engagement * 2, 0.20)
        )

        priority_level = "high" if viral_probability >= 0.70 or engagement_tier in ["high", "viral"] else "medium"

        recommendations.append(
            {
                "user_id": int(user_id),
                "influencer_id": safe_int(influencer_id),
                "recommendation_title": "ML-based content opportunity",
                "content_type": "topic",
                "description": (
                    f"Fokus pada pola konten dari {cluster_label}. "
                    f"Video terbaik memiliki {top_views:,} views, engagement {top_engagement:.4f}, "
                    f"dan viral probability {viral_probability:.4f}."
                ),
                "rationale": (
                    "[ml_daily_inference] Generated using RF viral prediction, "
                    "SVM engagement tier, KMeans content clustering, and Hugging Face NLP summary. "
                    f"NLP summary: {nlp_summary}"
                ),
                "suggested_keywords": to_json(suggested_keywords),
                "suggested_hashtags": to_json(suggested_hashtags),
                "suggested_duration": suggested_duration,
                "confidence_score": round(confidence_score, 4),
                "expected_engagement": round(max(avg_engagement, top_engagement * 0.75), 4),
                "priority_level": priority_level,
                "based_on_trend_id": None,
                "valid_until": VALID_UNTIL,
            }
        )

        best_hour = safe_int(top_video.get("published_hour"))
        best_day = safe_int(top_video.get("published_day_of_week"))

        recommendations.append(
            {
                "user_id": int(user_id),
                "influencer_id": safe_int(influencer_id),
                "recommendation_title": "ML-based posting format and timing",
                "content_type": "format",
                "description": (
                    f"Coba format konten berdurasi sekitar {suggested_duration} detik "
                    f"dan posting pada {get_day_name(best_day)} sekitar jam {best_hour}:00. "
                    f"Predicted engagement tier: {engagement_tier}."
                ),
                "rationale": (
                    "[ml_daily_inference] Generated from ML prediction output and historical posting pattern."
                ),
                "suggested_keywords": to_json(suggested_keywords[:5]),
                "suggested_hashtags": to_json(suggested_hashtags[:5]),
                "suggested_duration": suggested_duration,
                "confidence_score": round(clamp(confidence_score - 0.05), 4),
                "expected_engagement": round(avg_engagement, 4),
                "priority_level": "medium",
                "based_on_trend_id": None,
                "valid_until": VALID_UNTIL,
            }
        )

    return recommendations


def generate_posting_schedule_recommendations(user_id, features, top_n=5):
    recommendations = []

    if features.empty:
        return recommendations

    required_columns = [
        "influencer_id",
        "published_day_of_week",
        "published_hour",
        "views_num",
        "engagement_rate_num",
        "ml_viral_probability",
    ]

    working_df = features[required_columns].copy()
    working_df = working_df.dropna(subset=["influencer_id", "published_day_of_week", "published_hour"])

    if working_df.empty:
        return recommendations

    working_df.loc[:, "views_num"] = working_df["views_num"].fillna(0)
    working_df.loc[:, "engagement_rate_num"] = working_df["engagement_rate_num"].fillna(0)
    working_df.loc[:, "ml_viral_probability"] = working_df["ml_viral_probability"].fillna(0)

    grouped = (
        working_df
        .groupby(["influencer_id", "published_day_of_week", "published_hour"])
        .agg(
            total_videos=("views_num", "count"),
            avg_views=("views_num", "mean"),
            avg_engagement_rate=("engagement_rate_num", "mean"),
            avg_viral_probability=("ml_viral_probability", "mean"),
        )
        .reset_index()
    )

    grouped.loc[:, "score"] = (
        np.log10(grouped["avg_views"] + 1) * 0.30
        + grouped["avg_engagement_rate"] * 100 * 0.45
        + grouped["avg_viral_probability"] * 0.20
        + grouped["total_videos"] * 0.05
    )

    for influencer_id, group in grouped.groupby("influencer_id"):
        top_slots = group.sort_values("score", ascending=False).head(top_n)

        rank = 1

        for _, row in top_slots.iterrows():
            sample_size = safe_int(row["total_videos"])
            avg_engagement = safe_float(row["avg_engagement_rate"])
            avg_views = safe_float(row["avg_views"])
            avg_viral_probability = safe_float(row["avg_viral_probability"])

            confidence_score = clamp(
                0.50
                + min(sample_size / 20, 0.25)
                + min(avg_engagement, 0.15)
                + min(avg_viral_probability * 0.15, 0.15)
            )

            recommendations.append(
                {
                    "user_id": int(user_id),
                    "influencer_id": safe_int(influencer_id),
                    "day_of_week": safe_int(row["published_day_of_week"]),
                    "hour_of_day": safe_int(row["published_hour"]),
                    "expected_engagement_rate": round(avg_engagement, 4),
                    "expected_views": safe_int(avg_views),
                    "confidence_score": round(confidence_score, 4),
                    "rank_position": rank,
                    "based_on_sample_size": sample_size,
                    "reasoning": (
                        f"Based on {sample_size} historical videos and ML viral probability. "
                        f"{get_day_name(row['published_day_of_week'])} at {safe_int(row['published_hour'])}:00 "
                        f"has average engagement {avg_engagement:.4f}, average views {avg_views:.0f}, "
                        f"and average viral probability {avg_viral_probability:.4f}."
                    ),
                    "valid_from": TODAY,
                    "valid_until": VALID_UNTIL,
                }
            )

            rank += 1

    return recommendations


def calculate_hashtag_score(row):
    engagement = safe_float(row.get("avg_engagement_rate"))
    views = safe_float(row.get("views_count_num"))
    trending_score = safe_float(row.get("trending_score"))
    revenue_efficiency = safe_float(row.get("revenue_efficiency"))
    competition_level = str(row.get("competition_level") or "").lower()

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


def resolve_recommendation_type(row):
    revenue_efficiency = safe_float(row.get("revenue_efficiency"))
    trending_score = safe_float(row.get("trending_score"))
    is_rising = safe_int(row.get("is_rising"))

    if revenue_efficiency > 0:
        return "high_roi"

    if is_rising == 1 or trending_score > 0:
        return "trending"

    return "niche_match"


def generate_hashtag_recommendations(user_id, tracked_accounts, hashtag_candidates, top_n=10):
    recommendations = []

    if tracked_accounts.empty or hashtag_candidates.empty:
        return recommendations

    candidates = hashtag_candidates.copy()
    candidates.loc[:, "recommendation_score"] = candidates.apply(calculate_hashtag_score, axis=1)
    candidates = candidates.sort_values("recommendation_score", ascending=False).head(top_n)

    for _, account in tracked_accounts.iterrows():
        rank = 1

        for _, hashtag in candidates.iterrows():
            avg_engagement = safe_float(hashtag.get("avg_engagement_rate"))
            expected_reach = safe_int(hashtag.get("avg_views_per_video"))

            if expected_reach == 0:
                video_count = max(safe_int(hashtag.get("video_count_num")), 1)
                expected_reach = safe_int(safe_int(hashtag.get("views_count_num")) / video_count)

            recommendations.append(
                {
                    "user_id": int(user_id),
                    "influencer_id": safe_int(account["influencer_id"]),
                    "hashtag_pk": safe_int(hashtag["hashtag_pk"]),
                    "recommendation_score": round(safe_float(hashtag["recommendation_score"]), 4),
                    "rank_position": rank,
                    "expected_reach": expected_reach,
                    "expected_engagement_rate": round(avg_engagement, 4),
                    "competition_level": hashtag.get("competition_level"),
                    "reasoning": (
                        f"#{hashtag.get('tag_title')} recommended based on engagement, trending score, "
                        "competition level, and historical hashtag volume."
                    ),
                    "recommendation_type": resolve_recommendation_type(hashtag),
                    "valid_until": VALID_UNTIL,
                    "model_version": MODEL_VERSION,
                }
            )

            rank += 1

    return recommendations


def load_influencer_daily_history(engine, influencer_id):
    query = text("""
        SELECT
            DATE(s.snapshot_at) AS snap_date,
            SUM(s.views_num) AS total_views,
            SUM(s.likes_num) AS total_likes,
            SUM(s.comments_num) AS total_comments,
            SUM(s.shares_num) AS total_shares
        FROM video_metrics_snapshot s
        INNER JOIN videos_echotik v ON v.video_pk = s.video_pk
        WHERE v.influencer_id = :influencer_id
        GROUP BY DATE(s.snapshot_at)
        ORDER BY snap_date
    """)
    return pd.read_sql(query, engine, params={"influencer_id": influencer_id})


def prepare_history_sequence(history_df, seq_len=14):
    if history_df.empty:
        return np.zeros((seq_len, 4))

    history_df = history_df.drop_duplicates(subset=["snap_date"]).sort_values("snap_date").reset_index(drop=True)
    metrics = history_df[["total_views", "total_likes", "total_comments", "total_shares"]].values.astype(float)

    if len(metrics) == 1:
        return np.repeat(metrics, seq_len, axis=0)

    old_indices = np.arange(len(metrics))
    new_indices = np.linspace(0, len(metrics) - 1, seq_len)

    interpolated = np.zeros((seq_len, 4))
    for col in range(4):
        interpolated[:, col] = np.interp(new_indices, old_indices, metrics[:, col])

    return interpolated


def generate_lstm_forecasts(engine, user_id, tracked_accounts, artifacts):
    forecast_records = []

    if tracked_accounts.empty or artifacts.lstm_model is None:
        print("[LSTM FORECAST] Skip: Tracked accounts empty or LSTM model not loaded.")
        return forecast_records

    for _, account in tracked_accounts.iterrows():
        influencer_id = safe_int(account["influencer_id"])

        # Load metrics history
        history_df = load_influencer_daily_history(engine, influencer_id)
        history_seq = prepare_history_sequence(history_df, seq_len=14)

        # Log scale
        log_history = np.log1p(history_seq)

        # PyTorch inference
        x_tensor = torch.tensor([log_history], dtype=torch.float32)
        with torch.no_grad():
            pred_tensor = artifacts.lstm_model(x_tensor)
            pred_flat = pred_tensor.numpy()[0]

        pred_reshaped = pred_flat.reshape(7, 4)
        pred_metrics = np.expm1(pred_reshaped)
        pred_metrics = np.clip(pred_metrics, 0, None)

        # Save records for the next 7 days
        for day in range(7):
            forecast_date = TODAY + timedelta(days=day + 1)
            forecast_records.append({
                "user_id": int(user_id),
                "influencer_id": influencer_id,
                "forecast_date": forecast_date,
                "predicted_views": int(round(float(pred_metrics[day, 0]))),
                "predicted_likes": int(round(float(pred_metrics[day, 1]))),
                "predicted_comments": int(round(float(pred_metrics[day, 2]))),
                "predicted_shares": int(round(float(pred_metrics[day, 3]))),
                "model_version": MODEL_VERSION
            })

    return forecast_records


def insert_lstm_forecasts(conn, forecast_records):
    if not forecast_records:
        return 0

    sql = text("""
        INSERT INTO ml_engagement_forecasts (
            user_id, influencer_id, forecast_date,
            predicted_views, predicted_likes, predicted_comments, predicted_shares,
            model_version
        ) VALUES (
            :user_id, :influencer_id, :forecast_date,
            :predicted_views, :predicted_likes, :predicted_comments, :predicted_shares,
            :model_version
        )
    """)

    conn.execute(sql, forecast_records)
    return len(forecast_records)


def delete_existing_lstm_forecasts(conn, user_id):
    conn.execute(
        text("DELETE FROM ml_engagement_forecasts WHERE user_id = :user_id"),
        {"user_id": user_id}
    )


def delete_existing_outputs(conn, user_id):
    conn.execute(
        text(
            """
            DELETE FROM content_recommendations
            WHERE user_id = :user_id
              AND valid_until >= CURDATE()
              AND rationale LIKE '%[ml_daily_inference]%'
            """
        ),
        {"user_id": user_id},
    )

    conn.execute(
        text(
            """
            DELETE FROM hashtag_recommendations
            WHERE user_id = :user_id
              AND valid_until >= CURDATE()
              AND model_version = :model_version
            """
        ),
        {"user_id": user_id, "model_version": MODEL_VERSION},
    )

    conn.execute(
        text(
            """
            DELETE FROM posting_schedule_recommendations
            WHERE user_id = :user_id
              AND valid_from = :valid_from
            """
        ),
        {"user_id": user_id, "valid_from": TODAY},
    )


def insert_content_recommendations(conn, recommendations):
    if not recommendations:
        return 0

    sql = text(
        """
        INSERT INTO content_recommendations (
            user_id,
            influencer_id,
            recommendation_title,
            content_type,
            description,
            rationale,
            suggested_keywords,
            suggested_hashtags,
            suggested_duration,
            confidence_score,
            expected_engagement,
            priority_level,
            based_on_trend_id,
            valid_until
        )
        VALUES (
            :user_id,
            :influencer_id,
            :recommendation_title,
            :content_type,
            :description,
            :rationale,
            :suggested_keywords,
            :suggested_hashtags,
            :suggested_duration,
            :confidence_score,
            :expected_engagement,
            :priority_level,
            :based_on_trend_id,
            :valid_until
        )
        """
    )

    conn.execute(sql, recommendations)

    return len(recommendations)


def insert_posting_schedule_recommendations(conn, recommendations):
    if not recommendations:
        return 0

    sql = text(
        """
        INSERT INTO posting_schedule_recommendations (
            user_id,
            influencer_id,
            day_of_week,
            hour_of_day,
            expected_engagement_rate,
            expected_views,
            confidence_score,
            rank_position,
            based_on_sample_size,
            reasoning,
            valid_from,
            valid_until
        )
        VALUES (
            :user_id,
            :influencer_id,
            :day_of_week,
            :hour_of_day,
            :expected_engagement_rate,
            :expected_views,
            :confidence_score,
            :rank_position,
            :based_on_sample_size,
            :reasoning,
            :valid_from,
            :valid_until
        )
        """
    )

    conn.execute(sql, recommendations)

    return len(recommendations)


def insert_hashtag_recommendations(conn, recommendations):
    if not recommendations:
        return 0

    sql = text(
        """
        INSERT INTO hashtag_recommendations (
            user_id,
            influencer_id,
            hashtag_pk,
            recommendation_score,
            rank_position,
            expected_reach,
            expected_engagement_rate,
            competition_level,
            reasoning,
            recommendation_type,
            valid_until,
            model_version
        )
        VALUES (
            :user_id,
            :influencer_id,
            :hashtag_pk,
            :recommendation_score,
            :rank_position,
            :expected_reach,
            :expected_engagement_rate,
            :competition_level,
            :reasoning,
            :recommendation_type,
            :valid_until,
            :model_version
        )
        """
    )

    conn.execute(sql, recommendations)

    return len(recommendations)

def build_ml_prediction_rows(user_id, features):
    """
    Build rows untuk table ml_predictions dari output RF, SVM, dan KMeans.
    Table ini menyimpan prediksi per video.
    """
    prediction_rows = []

    if features.empty:
        return prediction_rows

    for _, row in features.iterrows():
        video_pk = safe_int(row.get("video_pk"), default=None)
        influencer_id = safe_int(row.get("influencer_id"), default=None)

        if video_pk is None or influencer_id is None:
            continue

        raw_response = {
            "viral": {
                "viral_prediction": safe_int(row.get("ml_viral_prediction")),
                "viral_probability": safe_float(row.get("ml_viral_probability")),
                "model": "rf_viral_prediction",
                "model_version": MODEL_VERSION,
            },
            "engagement": {
                "engagement_tier": safe_str(row.get("ml_engagement_tier"), "unknown"),
                "confidence": safe_float(row.get("ml_engagement_confidence")),
                "model": "svm_engagement_tier",
                "model_version": MODEL_VERSION,
            },
            "cluster": {
                "cluster_id": safe_int(row.get("ml_content_cluster")),
                "cluster_label": safe_str(row.get("ml_content_cluster_label"), "cluster_0"),
                "model": "kmeans_content_cluster",
                "model_version": MODEL_VERSION,
            },
        }

        prediction_rows.append(
            {
                "user_id": int(user_id),
                "video_pk": video_pk,
                "influencer_id": influencer_id,
                "viral_prediction": safe_int(row.get("ml_viral_prediction")),
                "viral_probability": round(safe_float(row.get("ml_viral_probability")), 6),
                "engagement_tier": safe_str(row.get("ml_engagement_tier"), "unknown"),
                "engagement_confidence": round(safe_float(row.get("ml_engagement_confidence")), 6),
                "cluster_id": safe_int(row.get("ml_content_cluster")),
                "cluster_label": safe_str(row.get("ml_content_cluster_label"), "cluster_0"),
                "model_version": MODEL_VERSION,
                "source": "spring_boot_ml_api",
                "raw_response": to_json(raw_response),
            }
        )

    return prediction_rows


def upsert_ml_predictions(conn, prediction_rows):
    """
    Insert/update ml_predictions.
    Aman di-run berkali-kali karena table punya UNIQUE:
    user_id + video_pk + model_version.
    """
    if not prediction_rows:
        return 0

    sql = text(
        """
        INSERT INTO ml_predictions (
            user_id,
            video_pk,
            influencer_id,
            viral_prediction,
            viral_probability,
            engagement_tier,
            engagement_confidence,
            cluster_id,
            cluster_label,
            model_version,
            source,
            raw_response,
            predicted_at
        )
        VALUES (
            :user_id,
            :video_pk,
            :influencer_id,
            :viral_prediction,
            :viral_probability,
            :engagement_tier,
            :engagement_confidence,
            :cluster_id,
            :cluster_label,
            :model_version,
            :source,
            :raw_response,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            influencer_id = VALUES(influencer_id),
            viral_prediction = VALUES(viral_prediction),
            viral_probability = VALUES(viral_probability),
            engagement_tier = VALUES(engagement_tier),
            engagement_confidence = VALUES(engagement_confidence),
            cluster_id = VALUES(cluster_id),
            cluster_label = VALUES(cluster_label),
            source = VALUES(source),
            raw_response = VALUES(raw_response),
            predicted_at = NOW()
        """
    )

    conn.execute(sql, prediction_rows)

    return len(prediction_rows)

def tag_new_videos(engine, artifacts):
    print("[CATEGORY TAGGING] Checking for untagged videos...")

    # Query to find videos in feature store that don't have tags yet
    query = """
        SELECT video_pk, title_full, hashtag_text, nlp_text
        FROM vw_ml_feature_store
        WHERE video_pk NOT IN (SELECT video_pk FROM video_category_tags)
    """
    try:
        df_untagged = pd.read_sql(query, engine)
    except Exception as e:
        print(f"[CATEGORY TAGGING ERROR] Failed to query untagged videos: {e}")
        return

    if df_untagged.empty:
        print("[CATEGORY TAGGING] No untagged videos found.")
        return

    print(f"[CATEGORY TAGGING] Found {len(df_untagged)} untagged videos.")
    tagged_records = []

    use_ml = artifacts.category_classifier is not None
    if use_ml:
        print("[CATEGORY TAGGING] Using ML Category Classifier...")
    else:
        print("[CATEGORY TAGGING] ML Category Classifier not loaded. Using rule-based fallback...")
        try:
            from inference.classify_rules import classify_video
        except ImportError:
            from classify_rules import classify_video

    for _, row in df_untagged.iterrows():
        video_pk = int(row["video_pk"])
        title = str(row["title_full"]) if row["title_full"] else ""
        hashtags = str(row["hashtag_text"]) if row["hashtag_text"] else ""
        nlp_text = str(row["nlp_text"]) if row["nlp_text"] else ""

        if use_ml:
            combined_text = f"{title} {hashtags} {nlp_text}".strip()
            category = str(artifacts.category_classifier.predict([combined_text])[0])

            confidence = 0.85
            try:
                model = artifacts.category_classifier
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
            except Exception:
                pass

            tagging_method = "ml_classifier"
        else:
            category, confidence = classify_video(title, hashtags, nlp_text)
            tagging_method = "rule_based"

        tagged_records.append({
            "video_pk": video_pk,
            "core_category": category,
            "confidence_score": confidence,
            "tagging_method": tagging_method,
            "model_version": MODEL_VERSION
        })

    # Bulk insert new tags
    sql_insert = text("""
        INSERT INTO video_category_tags (
            video_pk, core_category, confidence_score, tagging_method, model_version
        ) VALUES (
            :video_pk, :core_category, :confidence_score, :tagging_method, :model_version
        )
    """)

    try:
        with engine.begin() as conn:
            conn.execute(sql_insert, tagged_records)
        print(f"[CATEGORY TAGGING SUCCESS] Tagged {len(tagged_records)} new videos using {tagging_method}.")
    except Exception as e:
        print(f"[CATEGORY TAGGING ERROR] Failed to save tagged records: {e}")


def run_inference():
    print("[START] ML artifact-based daily inference started")
    print(f"[CONFIG] ENV_FILE={ENV_FILE}")
    print(f"[CONFIG] MODEL_VERSION={MODEL_VERSION}")
    print(f"[CONFIG] VALID_UNTIL={VALID_UNTIL}")
    print(f"[CONFIG] ENABLE_NLP_SUMMARY={ENABLE_NLP_SUMMARY}")

    engine = get_engine()
    artifacts = MLArtifacts()

    # Tag any new/untagged videos first
    tag_new_videos(engine, artifacts)

    users = load_active_users(engine)

    if users.empty:
        print("[STOP] No active tracked users found.")
        return

    hashtag_candidates = load_hashtag_candidates(engine, limit=50)

    print(f"[DATA] Active users loaded: {len(users)}")
    print(f"[DATA] Hashtag candidates loaded: {len(hashtag_candidates)}")

    total_predictions = 0
    total_content_recs = 0
    total_schedule_recs = 0
    total_hashtag_recs = 0
    total_forecast_recs = 0

    for _, user_row in users.iterrows():
        user_id = safe_int(user_row["user_id"])

        print(f"\n[USER] Processing user_id={user_id}")

        features = load_user_features(engine, user_id)
        tracked_accounts = load_user_tracked_accounts(engine, user_id)

        print(f"[DATA] Raw feature rows: {len(features)}")
        print(f"[DATA] Tracked accounts: {len(tracked_accounts)}")

        if features.empty:
            print(f"[SKIP] No feature data found for user_id={user_id}")
            continue

        features_with_predictions = add_ml_predictions(features, artifacts)

        print("[PREDICT] Prediction columns added:")
        print(" - ml_viral_prediction")
        print(" - ml_viral_probability")
        print(" - ml_engagement_tier")
        print(" - ml_content_cluster_label")

        content_recs = generate_content_recommendations(
            user_id=user_id,
            features=features_with_predictions,
            artifacts=artifacts,
        )

        schedule_recs = generate_posting_schedule_recommendations(
            user_id=user_id,
            features=features_with_predictions,
        )

        hashtag_recs = generate_hashtag_recommendations(
            user_id=user_id,
            tracked_accounts=tracked_accounts,
            hashtag_candidates=hashtag_candidates,
        )

        prediction_rows = build_ml_prediction_rows(
            user_id=user_id,
            features=features_with_predictions,
        )

        forecast_recs = generate_lstm_forecasts(
            engine=engine,
            user_id=user_id,
            tracked_accounts=tracked_accounts,
            artifacts=artifacts,
        )

        with engine.begin() as conn:
            delete_existing_outputs(conn, user_id)
            delete_existing_lstm_forecasts(conn, user_id)

            inserted_predictions = upsert_ml_predictions(conn, prediction_rows)
            inserted_content = insert_content_recommendations(conn, content_recs)
            inserted_schedule = insert_posting_schedule_recommendations(conn, schedule_recs)
            inserted_hashtag = insert_hashtag_recommendations(conn, hashtag_recs)
            inserted_forecasts = insert_lstm_forecasts(conn, forecast_recs)

        total_predictions += inserted_predictions
        total_content_recs += inserted_content
        total_schedule_recs += inserted_schedule
        total_hashtag_recs += inserted_hashtag
        total_forecast_recs += inserted_forecasts

        print(f"[SAVE] ml_predictions upserted: {inserted_predictions}")
        print(f"[SAVE] content_recommendations inserted: {inserted_content}")
        print(f"[SAVE] posting_schedule_recommendations inserted: {inserted_schedule}")
        print(f"[SAVE] hashtag_recommendations inserted: {inserted_hashtag}")
        print(f"[SAVE] ml_engagement_forecasts inserted: {inserted_forecasts}")

    print("\n[DONE] ML artifact-based daily inference completed")
    print(f"[SUMMARY] Total ML predictions upserted: {total_predictions}")
    print(f"[SUMMARY] Total content recommendations: {total_content_recs}")
    print(f"[SUMMARY] Total posting schedule recommendations: {total_schedule_recs}")
    print(f"[SUMMARY] Total hashtag recommendations: {total_hashtag_recs}")
    print(f"[SUMMARY] Total LSTM forecasts inserted: {total_forecast_recs}")


if __name__ == "__main__":
    run_inference()