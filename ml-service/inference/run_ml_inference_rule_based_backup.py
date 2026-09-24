import json
import math
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text


# Agar script bisa import db.py dari root ml-service
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from db import get_engine  # noqa: E402


load_dotenv(ROOT_DIR / ".env")


MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0")
VALID_DAYS = int(os.getenv("VALID_DAYS", "14"))

TODAY = date.today()
VALID_UNTIL = TODAY + timedelta(days=VALID_DAYS)


STOPWORDS = {
    "yang", "dan", "di", "ke", "dari", "ini", "itu", "untuk", "dengan",
    "atau", "pada", "the", "a", "an", "is", "are", "to", "of", "for",
    "in", "on", "by", "as", "at", "be", "was", "were", "akan", "bisa",
    "kamu", "aku", "kita", "mereka", "nya", "dong", "nih", "deh",
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


def clamp(value, min_value=0.0, max_value=1.0):
    return max(min_value, min(max_value, value))


def to_json(value):
    return json.dumps(value, ensure_ascii=False)


def extract_keywords(text_series, limit=8):
    """
    Keyword extractor sederhana untuk MVP.
    Nanti bisa diganti TF-IDF / Hugging Face / IndoBERT pada phase NLP.
    """
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


def ensure_model_registry(engine):
    """
    Karena model .pkl belum ada di Phase 8, kita register dulu rule-based recommender.
    Nanti setelah Phase 9 training selesai, registry ini bisa diganti model asli.
    """
    sql_check = text(
        """
        SELECT model_id
        FROM ml_model_registry
        WHERE model_name = :model_name
          AND version = :version
        LIMIT 1
        """
    )

    sql_insert = text(
        """
        INSERT INTO ml_model_registry (
            model_name,
            model_type,
            version,
            artifact_path,
            artifact_size_bytes,
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
            training_data_size,
            training_duration_sec,
            hyperparameters,
            feature_columns,
            trained_at,
            deployed_at,
            is_active,
            notes
        )
        VALUES (
            :model_name,
            :model_type,
            :version,
            :artifact_path,
            :artifact_size_bytes,
            :accuracy_score,
            :precision_score,
            :recall_score,
            :f1_score,
            :training_data_size,
            :training_duration_sec,
            :hyperparameters,
            :feature_columns,
            NOW(),
            NOW(),
            1,
            :notes
        )
        """
    )

    feature_columns = [
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
        "avg_hashtag_engagement_rate",
        "views_num",
        "engagement_rate_num",
        "gmv_usd",
    ]

    with engine.begin() as conn:
        existing = conn.execute(
            sql_check,
            {
                "model_name": "rule_based_daily_inference",
                "version": MODEL_VERSION,
            },
        ).fetchone()

        if existing:
            print(f"[MODEL] Registry already exists: rule_based_daily_inference {MODEL_VERSION}")
            return

        conn.execute(
            sql_insert,
            {
                "model_name": "rule_based_daily_inference",
                "model_type": "recommender",
                "version": MODEL_VERSION,
                "artifact_path": "rule_based_no_artifact",
                "artifact_size_bytes": 0,
                "accuracy_score": None,
                "precision_score": None,
                "recall_score": None,
                "f1_score": None,
                "training_data_size": 0,
                "training_duration_sec": 0,
                "hyperparameters": to_json({"type": "rule_based_mvp"}),
                "feature_columns": to_json(feature_columns),
                "notes": "MVP rule-based inference before trained ML artifacts are available.",
            },
        )

        print(f"[MODEL] Registry created: rule_based_daily_inference {MODEL_VERSION}")


def load_active_users(engine):
    sql = """
        SELECT DISTINCT user_id
        FROM tracked_accounts
        WHERE is_active = 1
        ORDER BY user_id
    """

    return pd.read_sql(sql, engine)


def load_user_features(engine, user_id):
    """
    Data utama inference.
    View vw_ml_feature_store sudah berisi latest video + engagement + hashtag aggregate.
    """
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
    """
    Candidate hashtag diambil dari master hashtag + BI summary + latest trending history.
    """
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


def generate_content_recommendations(user_id, features):
    """
    Membuat rekomendasi konten per influencer berdasarkan top-performing video.
    Output masuk ke content_recommendations.
    """
    recommendations = []

    if features.empty:
        return recommendations

    for influencer_id, group in features.groupby("influencer_id"):
        group = group.copy()

        group["engagement_rate_num"] = group["engagement_rate_num"].fillna(0)
        group["views_num"] = group["views_num"].fillna(0)
        group["duration_seconds"] = group["duration_seconds"].fillna(0)

        top_video = group.sort_values(
            by=["engagement_rate_num", "views_num"],
            ascending=[False, False],
        ).iloc[0]

        avg_engagement = safe_float(group["engagement_rate_num"].mean())
        avg_views = safe_float(group["views_num"].mean())
        top_engagement = safe_float(top_video["engagement_rate_num"])
        top_views = safe_int(top_video["views_num"])

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

        confidence_score = clamp(0.55 + (len(group) / 100) + (top_engagement * 2))
        priority_level = "high" if top_engagement >= 0.05 else "medium"

        recommendations.append(
            {
                "user_id": int(user_id),
                "influencer_id": safe_int(influencer_id),
                "recommendation_title": "Scale top-performing content theme",
                "content_type": "topic",
                "description": (
                    "Buat lebih banyak konten dengan tema yang mirip dengan video berperforma terbaik. "
                    f"Video tertinggi memiliki {top_views:,} views dan engagement rate {top_engagement:.4f}."
                ),
                "rationale": (
                    "[ml_daily_inference] Recommendation generated from top video performance, "
                    f"average account views {avg_views:.2f}, and average engagement {avg_engagement:.4f}."
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
                "recommendation_title": "Optimize posting format and timing",
                "content_type": "format",
                "description": (
                    f"Gunakan durasi sekitar {suggested_duration} detik dan coba posting pada "
                    f"{get_day_name(best_day)} sekitar jam {best_hour}:00 berdasarkan pola video terbaik."
                ),
                "rationale": (
                    "[ml_daily_inference] Recommendation generated from historical duration, "
                    "posting hour, and engagement performance."
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
    """
    Membuat rekomendasi jadwal posting per influencer.
    Output masuk ke posting_schedule_recommendations.
    """
    recommendations = []

    if features.empty:
        return recommendations

    required_columns = [
        "influencer_id",
        "published_day_of_week",
        "published_hour",
        "views_num",
        "engagement_rate_num",
    ]

    working_df = features[required_columns].copy()
    working_df = working_df.dropna(subset=["influencer_id", "published_day_of_week", "published_hour"])

    if working_df.empty:
        return recommendations

    working_df["views_num"] = working_df["views_num"].fillna(0)
    working_df["engagement_rate_num"] = working_df["engagement_rate_num"].fillna(0)

    grouped = (
        working_df
        .groupby(["influencer_id", "published_day_of_week", "published_hour"])
        .agg(
            total_videos=("views_num", "count"),
            avg_views=("views_num", "mean"),
            avg_engagement_rate=("engagement_rate_num", "mean"),
        )
        .reset_index()
    )

    grouped["score"] = (
        np.log10(grouped["avg_views"] + 1) * 0.35
        + grouped["avg_engagement_rate"] * 100 * 0.55
        + grouped["total_videos"] * 0.10
    )

    for influencer_id, group in grouped.groupby("influencer_id"):
        top_slots = group.sort_values("score", ascending=False).head(top_n)

        rank = 1

        for _, row in top_slots.iterrows():
            sample_size = safe_int(row["total_videos"])
            avg_engagement = safe_float(row["avg_engagement_rate"])
            avg_views = safe_float(row["avg_views"])

            confidence_score = clamp(0.50 + min(sample_size / 20, 0.30) + avg_engagement)

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
                        f"Based on {sample_size} historical videos. "
                        f"{get_day_name(row['published_day_of_week'])} at {safe_int(row['published_hour'])}:00 "
                        f"has average engagement {avg_engagement:.4f} and average views {avg_views:.0f}."
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
    """
    Membuat rekomendasi hashtag per tracked account.
    Output masuk ke hashtag_recommendations.
    """
    recommendations = []

    if tracked_accounts.empty or hashtag_candidates.empty:
        return recommendations

    candidates = hashtag_candidates.copy()
    candidates["recommendation_score"] = candidates.apply(calculate_hashtag_score, axis=1)
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


def delete_existing_outputs(conn, user_id):
    """
    Membersihkan output ML aktif agar script bisa di-run berkali-kali tanpa duplicate.
    Data lama yang bukan hasil ml_daily_inference tidak ikut dihapus untuk content recommendation.
    """
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


def run_inference():
    print("[START] ML daily inference started")
    print(f"[CONFIG] MODEL_VERSION={MODEL_VERSION}")
    print(f"[CONFIG] VALID_UNTIL={VALID_UNTIL}")

    engine = get_engine()

    ensure_model_registry(engine)

    users = load_active_users(engine)

    if users.empty:
        print("[STOP] No active tracked users found.")
        return

    hashtag_candidates = load_hashtag_candidates(engine, limit=50)
    print(f"[DATA] Hashtag candidates loaded: {len(hashtag_candidates)}")

    total_content_recs = 0
    total_schedule_recs = 0
    total_hashtag_recs = 0

    for _, user_row in users.iterrows():
        user_id = safe_int(user_row["user_id"])

        print(f"\n[USER] Processing user_id={user_id}")

        features = load_user_features(engine, user_id)
        tracked_accounts = load_user_tracked_accounts(engine, user_id)

        print(f"[DATA] Feature rows: {len(features)}")
        print(f"[DATA] Tracked accounts: {len(tracked_accounts)}")

        if features.empty:
            print(f"[SKIP] No feature data found for user_id={user_id}")
            continue

        content_recs = generate_content_recommendations(user_id, features)
        schedule_recs = generate_posting_schedule_recommendations(user_id, features)
        hashtag_recs = generate_hashtag_recommendations(
            user_id=user_id,
            tracked_accounts=tracked_accounts,
            hashtag_candidates=hashtag_candidates,
        )

        with engine.begin() as conn:
            delete_existing_outputs(conn, user_id)

            inserted_content = insert_content_recommendations(conn, content_recs)
            inserted_schedule = insert_posting_schedule_recommendations(conn, schedule_recs)
            inserted_hashtag = insert_hashtag_recommendations(conn, hashtag_recs)

        total_content_recs += inserted_content
        total_schedule_recs += inserted_schedule
        total_hashtag_recs += inserted_hashtag

        print(f"[SAVE] content_recommendations inserted: {inserted_content}")
        print(f"[SAVE] posting_schedule_recommendations inserted: {inserted_schedule}")
        print(f"[SAVE] hashtag_recommendations inserted: {inserted_hashtag}")

    print("\n[DONE] ML daily inference completed")
    print(f"[SUMMARY] Total content recommendations: {total_content_recs}")
    print(f"[SUMMARY] Total posting schedule recommendations: {total_schedule_recs}")
    print(f"[SUMMARY] Total hashtag recommendations: {total_hashtag_recs}")


if __name__ == "__main__":
    run_inference()