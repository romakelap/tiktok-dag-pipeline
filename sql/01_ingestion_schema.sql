-- =====================================================
-- INGESTION SCHEMA — Staging & Audit Tables
-- Run ini SEKALI sebelum DAG 2 di-trigger
-- =====================================================

USE tiktok_oltp;

-- =====================================================
-- 1. ADD data_source column ke videos_echotik
-- Untuk distinguish video dari library vs shop endpoint
-- =====================================================
ALTER TABLE videos_echotik
ADD COLUMN IF NOT EXISTS data_source ENUM('library', 'shop', 'manual') 
    NOT NULL DEFAULT 'library' 
    COMMENT 'Sumber data: library API, shop/selling API, atau manual upload'
AFTER echotik_video_id;

ALTER TABLE videos_echotik
ADD INDEX IF NOT EXISTS idx_video_source (data_source);


-- =====================================================
-- 2. STAGING TABLE — videos_echotik_staging
-- Temporary table untuk validasi sebelum masuk production
-- =====================================================
DROP TABLE IF EXISTS videos_echotik_staging;
CREATE TABLE videos_echotik_staging (
    staging_id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    
    -- Source identifiers (untuk UPSERT)
    echotik_video_id        VARCHAR(50) NOT NULL,
    data_source             ENUM('library', 'shop', 'manual') NOT NULL DEFAULT 'library',
    
    -- Video core fields
    title_full              TEXT,
    title_brief             VARCHAR(255),
    cover_url               VARCHAR(500),
    video_url               VARCHAR(500),
    influencer_id           BIGINT UNSIGNED,
    influencer_name         VARCHAR(150),
    influencer_unique_id    VARCHAR(100),
    influencer_avatar_url   VARCHAR(500),
    follower_count_raw      VARCHAR(20),
    follower_count_num      BIGINT UNSIGNED,
    influencer_region       VARCHAR(10),
    sales_flag              TINYINT UNSIGNED DEFAULT 0,
    category_name           VARCHAR(50) NULL,
    
    
    -- Video metrics
    duration_raw            VARCHAR(10),
    duration_seconds        INT UNSIGNED,
    duration_bucket         VARCHAR(20),
    views_num               BIGINT UNSIGNED DEFAULT 0,
    likes_num               BIGINT UNSIGNED DEFAULT 0,
    comments_num            BIGINT UNSIGNED DEFAULT 0,
    shares_num              BIGINT UNSIGNED DEFAULT 0,
    engagement_rate_raw     VARCHAR(10),
    engagement_rate_num     DECIMAL(6,4),
    likes_per_views_raw     VARCHAR(10),
    likes_per_views_num     DECIMAL(6,4),
    
    -- Sales & GMV (untuk video dari shop endpoint)
    sales_count             INT UNSIGNED DEFAULT 0,
    gmv_usd                 DECIMAL(15,2) DEFAULT 0.00,
    total_sale_cnt_num      INT UNSIGNED DEFAULT 0,
    total_gmv_amt_usd       DECIMAL(15,2) DEFAULT 0.00,
    total_gmv_amt_local     DECIMAL(15,2) DEFAULT 0.00,
    total_gmv_amt_currency  VARCHAR(10),
    total_views_count_num   BIGINT UNSIGNED DEFAULT 0,
    interact_ratio_num      DECIMAL(6,4),
    
    -- Flags
    is_ai_video             TINYINT(1) DEFAULT 0,
    is_promote              TINYINT(1) DEFAULT 0,
    is_latest               TINYINT(1) DEFAULT 0,
    is_delete               TINYINT(1) DEFAULT 0,
    
    -- Content analysis
    title_length            INT UNSIGNED,
    hashtag_count           TINYINT UNSIGNED DEFAULT 0,
    mention_count           TINYINT UNSIGNED DEFAULT 0,
    emoji_count             TINYINT UNSIGNED DEFAULT 0,
    has_question            TINYINT(1) DEFAULT 0,
    
    -- Dates
    published_at            DATETIME,
    
    -- Ingest metadata
    ingest_run_id           VARCHAR(50) NOT NULL,
    ingest_excel_file       VARCHAR(255),
    ingested_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Validation flags (set by db_validator)
    is_valid                TINYINT(1) DEFAULT 1,
    validation_error        VARCHAR(255),
    
    INDEX idx_stg_video_id (echotik_video_id),
    INDEX idx_stg_run (ingest_run_id),
    INDEX idx_stg_valid (is_valid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Staging untuk videos sebelum UPSERT ke production';


-- =====================================================
-- 3. STAGING TABLE — hashtags_echotik_staging
-- =====================================================
DROP TABLE IF EXISTS hashtags_echotik_staging;
CREATE TABLE hashtags_echotik_staging (
    staging_id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    
    echotik_tag_id          VARCHAR(50) NOT NULL,
    tag_title               VARCHAR(100),
    tag_title_brief         VARCHAR(100),
    region_id               VARCHAR(10),
    region_name             VARCHAR(100),
    
    video_count_raw         VARCHAR(20),
    video_count_num         BIGINT UNSIGNED,
    views_count_raw         VARCHAR(20),
    views_count_num         BIGINT UNSIGNED,
    likes_count_num         BIGINT UNSIGNED,
    comments_count_num      BIGINT UNSIGNED,
    shares_count_num        BIGINT UNSIGNED,
    favorites_count_num     BIGINT UNSIGNED,
    
    avg_views_per_video     DECIMAL(15,2),
    competition_level       VARCHAR(20),
    
    ingest_run_id           VARCHAR(50) NOT NULL,
    ingest_excel_file       VARCHAR(255),
    ingested_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    is_valid                TINYINT(1) DEFAULT 1,
    validation_error        VARCHAR(255),
    
    INDEX idx_stg_tag_id (echotik_tag_id),
    INDEX idx_stg_tag_run (ingest_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Staging untuk hashtags sebelum UPSERT';


-- =====================================================
-- 4. STAGING TABLE — video_metrics_snapshot_staging
-- =====================================================
DROP TABLE IF EXISTS video_metrics_snapshot_staging;
CREATE TABLE video_metrics_snapshot_staging (
    staging_id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    
    echotik_video_id        VARCHAR(50) NOT NULL,
    data_source             VARCHAR(20) NOT NULL,
    
    -- Metrics
    views_raw               VARCHAR(20),
    views_num               BIGINT UNSIGNED DEFAULT 0,
    likes_num               BIGINT UNSIGNED DEFAULT 0,
    comments_num            BIGINT UNSIGNED DEFAULT 0,
    shares_num              BIGINT UNSIGNED DEFAULT 0,
    engagement_rate_raw     VARCHAR(10),
    engagement_rate_num     DECIMAL(6,4),
    likes_per_views_raw     VARCHAR(10),
    likes_per_views_num     DECIMAL(6,4),
    
    -- Sales & GMV
    sales_count             INT UNSIGNED DEFAULT 0,
    gmv_usd                 DECIMAL(15,2) DEFAULT 0.00,
    gmv_local               DECIMAL(15,2) DEFAULT 0.00,
    gmv_local_currency      VARCHAR(10),
    
    -- From video_selling
    total_sale_cnt_num      INT UNSIGNED DEFAULT 0,
    total_gmv_amt_usd       DECIMAL(15,2) DEFAULT 0.00,
    total_views_count_num   BIGINT UNSIGNED DEFAULT 0,
    interact_ratio_num      DECIMAL(6,4),
    
    ingest_run_id           VARCHAR(50) NOT NULL,
    ingested_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    is_valid                TINYINT(1) DEFAULT 1,
    validation_error        VARCHAR(255),
    
    INDEX idx_stg_snap_video (echotik_video_id),
    INDEX idx_stg_snap_run (ingest_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Staging untuk video metrics snapshot';


-- =====================================================
-- 5. AUDIT LOG TABLE
-- Tracking semua ingestion runs untuk debugging & monitoring
-- =====================================================
DROP TABLE IF EXISTS ingest_audit_log;
CREATE TABLE ingest_audit_log (
    audit_id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id                  VARCHAR(50) NOT NULL,
    source_file             VARCHAR(255),
    
    -- Timing
    start_time              DATETIME NOT NULL,
    end_time                DATETIME,
    duration_sec            INT UNSIGNED,
    
    -- Counts: from Excel
    total_records_read      INT UNSIGNED DEFAULT 0,
    
    -- Counts: validation
    valid_records           INT UNSIGNED DEFAULT 0,
    invalid_records         INT UNSIGNED DEFAULT 0,
    
    -- Counts: per table
    videos_inserted         INT UNSIGNED DEFAULT 0,
    videos_updated          INT UNSIGNED DEFAULT 0,
    hashtags_inserted       INT UNSIGNED DEFAULT 0,
    hashtags_updated        INT UNSIGNED DEFAULT 0,
    snapshots_inserted      INT UNSIGNED DEFAULT 0,
    influencers_inserted    INT UNSIGNED DEFAULT 0,
    influencers_updated     INT UNSIGNED DEFAULT 0,
    
    -- Status
    status                  ENUM('SUCCESS', 'PARTIAL', 'FAILED') NOT NULL DEFAULT 'SUCCESS',
    error_message           TEXT,
    
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_audit_run (run_id),
    INDEX idx_audit_status (status),
    INDEX idx_audit_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Audit log untuk semua ingestion runs';


SELECT 'Ingestion schema setup complete!' AS status;
