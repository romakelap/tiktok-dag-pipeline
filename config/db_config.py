"""
Database Configuration untuk ingestion DAG.

Connection bisa di-config via Airflow Variable atau Airflow Connection.
"""

# Path patterns
EXCEL_INPUT_PATH = "/opt/airflow/raw-data/Excel"
EXCEL_PROCESSED_PATH = "/opt/airflow/raw-data/Excel/processed"
EXCEL_FAILED_PATH = "/opt/airflow/raw-data/Excel/failed"
EXCEL_FILE_PATTERN = "echotik_data_*.xlsx"

# Sheet names di Excel
EXCEL_SHEETS = {
    "video_library": "video_library",
    "hashtags": "hashtags",
    "video_selling": "video_selling",
    "validation_report": "validation_report",  # SKIP saat ingest
}

# DB target tables (production)
PRODUCTION_TABLES = {
    "regions": "regions",
    "influencers": "influencers",
    "videos": "videos_echotik",
    "hashtags": "hashtags_echotik",
    "video_hashtags": "video_hashtags_echotik",
    "video_metrics": "video_metrics_snapshot",
    "bi_video": "bi_video_revenue_summary",
    "bi_hashtag": "bi_hashtag_revenue_summary",
}

# Staging tables
STAGING_TABLES = {
    "videos": "videos_echotik_staging",
    "hashtags": "hashtags_echotik_staging",
    "snapshots": "video_metrics_snapshot_staging",
}

# Audit log table
AUDIT_LOG_TABLE = "ingest_audit_log"

# Batch size untuk bulk INSERT
BATCH_SIZE = 100

# Apakah pindah file ke processed/ setelah ingest
MOVE_PROCESSED_FILES = True

# Apakah auto-refresh BI summary setelah ingest
AUTO_REFRESH_BI = True

# Cleanup staging setelah ingest
# Option: 'truncate_immediately', 'keep_24h', 'keep_forever'
STAGING_CLEANUP = "truncate_immediately"
