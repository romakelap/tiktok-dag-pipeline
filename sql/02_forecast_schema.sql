-- =====================================================
-- FORECAST SCHEMA — LSTM Engagement Predictions
-- Run ini SEKALI untuk menyimpan proyeksi runtun waktu (time-series)
-- =====================================================

USE tiktok_oltp;

DROP TABLE IF EXISTS ml_engagement_forecasts;
CREATE TABLE ml_engagement_forecasts (
    forecast_id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id                 BIGINT UNSIGNED NOT NULL,
    influencer_id           BIGINT UNSIGNED NOT NULL,
    forecast_date           DATE NOT NULL,
    predicted_views         BIGINT UNSIGNED DEFAULT 0,
    predicted_likes         BIGINT UNSIGNED DEFAULT 0,
    predicted_comments      BIGINT UNSIGNED DEFAULT 0,
    predicted_shares        BIGINT UNSIGNED DEFAULT 0,
    model_version           VARCHAR(20) NOT NULL,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_fc_influencer (influencer_id),
    INDEX idx_fc_date (forecast_date),
    INDEX idx_fc_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Menyimpan hasil proyeksi 7 hari ke depan dari model LSTM';

SELECT 'Forecast schema setup complete!' AS status;
