-- =====================================================
-- FILE    : sql/03_feature_store_view.sql
-- PURPOSE : Definisi VIEW vw_ml_feature_store
--           Sumber fitur untuk semua model Machine Learning
-- AUTHOR  : Nico (reconstructed from parquet + source table analysis)
-- DATE    : 2026-06-09
--
-- SOURCE TABLES:
--   - videos_echotik              core video features
--   - influencers                 follower data & tier
--   - video_metrics_snapshot      latest engagement metrics (time-series)
--   - video_hashtags_echotik      video <-> hashtag join table
--   - hashtags_echotik            hashtag statistics
--   - bi_video_revenue_summary    revenue tier + latest GMV
--   - video_products              products linked to video
--
-- OUTPUT  : 53 kolom, urutan sesuai parquet ml_feature_store.parquet
--
-- LABEL THRESHOLDS (diverifikasi dari 10.373 baris data aktual):
--   label_is_viral        : views_num >= 1.000.000  (max non-viral = 999.880)
--   label_engagement_tier : viral  >= 1.000.000 views
--                           high   >= 0.05 engagement_rate
--                           medium >= 0.02 engagement_rate
--                           low    <  0.02 engagement_rate
--
-- DURATION BUCKET ENCODING:
--   0 = very_short (< 15 detik)
--   1 = short      (15–29 detik)
--   2 = medium     (30–59 detik)
--   3 = long       (60–119 detik)
--   4 = very_long  (>= 120 detik)
--
-- FOLLOWER TIER ENCODING:
--   1=nano | 2=micro | 3=mid | 4=macro | 5=mega
--
-- REVENUE TIER ENCODING:
--   0=no_sales | 1=low | 2=medium | 3=high | 4=top
-- =====================================================

CREATE OR REPLACE VIEW vw_ml_feature_store AS

SELECT

    -- =========================================================
    -- 1–4 IDENTIFIKASI
    -- =========================================================
    v.video_pk,
    v.echotik_video_id,
    v.data_source,
    v.influencer_id,
    i.unique_id                                             AS influencer_unique_id,
    i.display_name                                          AS influencer_display_name,

    -- =========================================================
    -- 7–9 KONTEN TEKS
    -- =========================================================
    v.title_full,
    v.title_brief,
    v.title_brief                                           AS nlp_text,

    -- =========================================================
    -- 10–21 FITUR NUMERIK — Profil & Karakteristik Video
    -- =========================================================
    COALESCE(i.follower_count_num, 0)                       AS follower_count_num,
    v.duration_seconds,
    v.title_length,
    v.hashtag_count,
    v.mention_count,
    v.emoji_count,
    v.has_question,
    v.published_day_of_week,
    v.published_hour,
    v.is_ai_video,
    v.is_promote,
    v.is_latest,

    -- =========================================================
    -- 22–28 FITUR NUMERIK — Encoded & Hashtag Stats
    -- =========================================================
    CASE
        WHEN v.duration_seconds < 15  THEN 0
        WHEN v.duration_seconds < 30  THEN 1
        WHEN v.duration_seconds < 60  THEN 2
        WHEN v.duration_seconds < 120 THEN 3
        ELSE                               4
    END                                                     AS duration_bucket_encoded,

    CASE i.follower_tier
        WHEN 'nano'  THEN 1
        WHEN 'micro' THEN 2
        WHEN 'mid'   THEN 3
        WHEN 'macro' THEN 4
        WHEN 'mega'  THEN 5
        ELSE 0
    END                                                     AS follower_tier_encoded,

    COALESCE(ha.linked_hashtag_count, 0)                    AS linked_hashtag_count,
    COALESCE(ha.avg_hashtag_views, 0)                       AS avg_hashtag_views,
    COALESCE(ha.avg_hashtag_video_count, 0)                 AS avg_hashtag_video_count,
    COALESCE(ha.avg_hashtag_engagement_rate, 0)             AS avg_hashtag_engagement_rate,
    COALESCE(ha.avg_hashtag_competition_score, 0)           AS avg_hashtag_competition_score,

    -- =========================================================
    -- 29 HASHTAG TEXT (teks gabungan semua hashtag video)
    -- =========================================================
    COALESCE(ha.hashtag_text, '')                           AS hashtag_text,

    -- =========================================================
    -- 30–36 METRIK PERFORMA (latest snapshot)
    -- =========================================================
    COALESCE(s.views_num, 0)                                AS views_num,
    COALESCE(s.likes_num, 0)                                AS likes_num,
    COALESCE(s.comments_num, 0)                             AS comments_num,
    COALESCE(s.shares_num, 0)                               AS shares_num,
    COALESCE(s.likes_per_views_num, 0)                      AS likes_per_views_num,
    COALESCE(s.engagement_rate_num, 0)                      AS engagement_rate_num,
    COALESCE(s.interact_ratio_num, 0)                       AS interact_ratio_num,

    -- =========================================================
    -- 37–44 METRIK E-COMMERCE
    -- =========================================================
    COALESCE(s.sales_count, 0)                              AS sales_count,
    COALESCE(s.sales_count, 0)                              AS total_sale_cnt_num,
    COALESCE(s.gmv_usd, 0)                                  AS gmv_usd,
    COALESCE(s.gmv_local, 0)                                AS gmv_local,
    COALESCE(bi.latest_gmv_usd, 0)                          AS total_gmv_amt_usd,

    CASE
        WHEN COALESCE(s.views_num, 0) > 0
        THEN ROUND(COALESCE(s.gmv_usd, 0) / s.views_num, 8)
        ELSE 0
    END                                                     AS gmv_per_view,

    CASE
        WHEN COALESCE(s.views_num, 0) > 0
        THEN LEAST(COALESCE(s.sales_count, 0) / s.views_num, 1.0)
        ELSE 0
    END                                                     AS sales_conversion_rate,

    CASE
        WHEN COALESCE(s.sales_count, 0) > 0
        THEN ROUND(COALESCE(s.gmv_usd, 0) / s.sales_count, 4)
        ELSE 0
    END                                                     AS avg_order_value_usd,

    -- =========================================================
    -- 45–46 DERIVED COLUMNS
    -- =========================================================
    TIMESTAMPDIFF(HOUR, v.published_at, NOW())              AS hours_since_published,

    CASE
        WHEN TIMESTAMPDIFF(HOUR, v.published_at, NOW()) > 0
        THEN ROUND(
            COALESCE(s.views_num, 0) /
            TIMESTAMPDIFF(HOUR, v.published_at, NOW()),
            4
        )
        ELSE 0
    END                                                     AS view_velocity,

    -- =========================================================
    -- 47–49 MONETISASI
    -- =========================================================
    COALESCE(pc.total_products_linked, 0)                   AS total_products_linked,

    -- Gabungan gmv (50%), produk linked (30%), status promosi (20%)
    LEAST(1.0,
        COALESCE(bi.latest_gmv_usd, 0) / 10000.0 * 0.5 +
        LEAST(COALESCE(pc.total_products_linked, 0), 5) / 5.0 * 0.3 +
        COALESCE(v.is_promote, 0) * 0.2
    )                                                       AS monetization_score,

    CASE COALESCE(bi.revenue_tier, 'no_sales')
        WHEN 'no_sales' THEN 0
        WHEN 'low'      THEN 1
        WHEN 'medium'   THEN 2
        WHEN 'high'     THEN 3
        WHEN 'top'      THEN 4
        ELSE 0
    END                                                     AS revenue_tier_encoded,

    -- =========================================================
    -- 50–51 LABEL GROUND TRUTH
    -- =========================================================
    CASE
        WHEN COALESCE(s.views_num, 0) >= 1000000 THEN 1
        ELSE 0
    END                                                     AS label_is_viral,

    CASE
        WHEN COALESCE(s.views_num, 0)           >= 1000000 THEN 'viral'
        WHEN COALESCE(s.engagement_rate_num, 0) >= 0.05    THEN 'high'
        WHEN COALESCE(s.engagement_rate_num, 0) >= 0.02    THEN 'medium'
        ELSE 'low'
    END                                                     AS label_engagement_tier,

    -- =========================================================
    -- 52–53 TIMESTAMP
    -- =========================================================
    v.published_at,
    s.snapshot_at                                           AS snapshot_at

-- =========================================================
-- FROM & JOINs
-- =========================================================
FROM videos_echotik v

-- Influencer profile
INNER JOIN influencers i
    ON i.influencer_id = v.influencer_id

-- Latest snapshot per video (ambil snapshot_at MAX per video_pk)
LEFT JOIN (
    SELECT s1.*
    FROM video_metrics_snapshot s1
    INNER JOIN (
        SELECT
            video_pk,
            MAX(snapshot_at) AS max_snapshot_at
        FROM video_metrics_snapshot
        GROUP BY video_pk
    ) s2
        ON  s1.video_pk    = s2.video_pk
        AND s1.snapshot_at = s2.max_snapshot_at
) s ON s.video_pk = v.video_pk

-- BI revenue summary
LEFT JOIN bi_video_revenue_summary bi
    ON bi.video_pk = v.video_pk

-- Hashtag aggregation per video
LEFT JOIN (
    SELECT
        vh.video_pk,
        COUNT(DISTINCT vh.hashtag_pk)                               AS linked_hashtag_count,
        GROUP_CONCAT(
            h.tag_title
            ORDER BY h.views_count_num DESC
            SEPARATOR ' '
        )                                                           AS hashtag_text,
        AVG(COALESCE(h.views_count_num, 0))                         AS avg_hashtag_views,
        AVG(COALESCE(h.video_count_num, 0))                         AS avg_hashtag_video_count,
        AVG(
            CASE
                WHEN COALESCE(h.views_count_num, 0) > 0
                THEN LEAST(
                    (COALESCE(h.likes_count_num,    0)
                   + COALESCE(h.comments_count_num, 0)
                   + COALESCE(h.shares_count_num,   0)
                    ) / h.views_count_num,
                    1.0
                )
                ELSE 0
            END
        )                                                           AS avg_hashtag_engagement_rate,
        AVG(
            CASE h.competition_level
                WHEN 'low'     THEN 0.25
                WHEN 'medium'  THEN 0.50
                WHEN 'high'    THEN 0.75
                WHEN 'extreme' THEN 1.00
                ELSE 0.50
            END
        )                                                           AS avg_hashtag_competition_score
    FROM video_hashtags_echotik vh
    INNER JOIN hashtags_echotik h ON h.hashtag_pk = vh.hashtag_pk
    GROUP BY vh.video_pk
) ha ON ha.video_pk = v.video_pk

-- Product count per video
LEFT JOIN (
    SELECT
        video_pk,
        COUNT(*) AS total_products_linked
    FROM video_products
    GROUP BY video_pk
) pc ON pc.video_pk = v.video_pk

WHERE v.is_delete = 0;




ALTER TABLE videos_echotik_staging 
ADD COLUMN influencer_avatar_url VARCHAR(500);
