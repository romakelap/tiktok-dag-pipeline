"""
Echotik API Configuration.

UPDATE v3:
- Pagination: page 1-7, per_page=20 (apply ke semua API)
- Bearer token auth
- Regional headers (x-region, x-currency, x-lang)
"""

# Base API URL
BASE_URL = "https://echotik.live"


# ============================================
# PAGINATION SETTINGS (apply ke semua API)
# ============================================
PAGINATION_CONFIG = {
    "per_page": 30,
    "start_page": 1,
    "end_page": 14,  # Fetch page 1 sampai 7
    
    # Throttling
    "delay_between_pages_min": 3,
    "delay_between_pages_max": 10,
    "delay_between_apis_min": 30,
    "delay_between_apis_max": 60,
    
    # Retry
    "request_timeout_seconds": 30,
    "max_retries": 3,
    "retry_backoff_seconds": 60,
}


# ============================================
# REGIONAL HEADERS (sesuai cURL browser Nico)
# Penting biar dapat GMV/sales data lengkap
# ============================================
REGIONAL_HEADERS = {
    "x-region": "ID",
    "x-currency": "IDR",
    "x-lang": "en-US",
    "x-secondary-currency": "CNY",
}


# ============================================
# API ENDPOINT CONFIGS
# ============================================

VIDEO_LIBRARY_CONFIG = {
    "name": "Video Library (All)",
    "slug": "video_library_all",
    "endpoint": "/api/v1/data/videos",
    "referer": "/data/videos",  # Referer path
}

HASHTAG_CONFIG = {
    "name": "Hashtag Library (Weekly)",
    "slug": "hashtags",
    "endpoint": "/api/v1/data/tags/leaderboard/top-hashtag",
    "referer": "/videos/leaderboard/top-hashtags",
    "time_type": "weekly",
}

VIDEO_SELLING_CONFIG = {
    "name": "Video Selling (Monthly)",
    "slug": "video_selling_all",
    "endpoint": "/api/v1/data/videos/leaderboard/sell-videos",
    "referer": "/videos/leaderboard/top-selling-videos",
    "time_type": "monthly",
}

# ============================================
# Output Paths
# Mounted: /Users/nicorevaldo/Desktop/STIKOM/TA/raw-data → /opt/airflow/raw-data
# ============================================
RAW_JSON_PATH = "/opt/airflow/raw-data/json"
EXCEL_OUTPUT_PATH = "/opt/airflow/raw-data/Excel"
