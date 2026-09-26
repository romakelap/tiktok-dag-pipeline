"""
============================================================
ECHOTIK DATA COLLECTION PIPELINE (v3)
============================================================

UPDATE v3:
- Pagination strict: page 1-7, per_page=20 (apply ke semua API)
- Bearer token auth (dari cURL browser Nico)
- Regional headers: x-region=ID, x-currency=IDR, x-lang=en-US
- Output ke /Users/nicorevaldo/Desktop/STIKOM/TA/raw-data/

API endpoints:
1. Video Library — /api/v1/data/videos
2. Hashtag Library — /api/v1/data/tags
3. Video Selling — /api/v1/data/videos/leaderboard/sell-videos

SCHEDULE: Setiap 1.5 jam (90 menit)

REQUIREMENT Airflow Variables:
- ECHOTIK_BEARER_TOKEN: bearer token dari browser
  Format: "3083623|ZMmLGWMl2x8v8Vbj5d8Yrcm1hP203rgujpcwvpZw"
- ECHOTIK_COOKIES: (optional) cookie string lengkap untuk fallback
- DISCORD_WEBHOOK_URL: webhook Discord
"""
import sys
import os
import json
import time
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models.param import Param

sys.path.insert(0, '/opt/airflow/plugins')
sys.path.insert(0, '/opt/airflow')

from hooks.echotik_client import EchotikAPIClient
from utils.parsers.echotik_parser import (
    parse_video_library_record,
    parse_hashtag_record,
    parse_video_selling_record,
)
from utils.transforms.validator import (
    validate_video_record,
    validate_hashtag_record,
    deduplicate_records,
    get_validation_summary,
)
from utils.transforms.excel_exporter import export_to_excel
from utils.monitoring.discord_notifier import DiscordNotifier

sys.path.insert(0, '/opt/airflow/config')
from categories_config import (
    VIDEO_LIBRARY_CONFIG,
    HASHTAG_CONFIG,
    VIDEO_SELLING_CONFIG,
    PAGINATION_CONFIG,
    REGIONAL_HEADERS,
    RAW_JSON_PATH,
    EXCEL_OUTPUT_PATH,
)


default_args = {
    'owner': 'nico',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================
# HELPERS
# ============================================
def _get_notifier():
    try:
        from utils.monitoring.discord_notifier import get_notifier
        return get_notifier()
    except Exception as e:
        logging.warning(f"Notifier not available: {e}")
        return None


def _get_bearer_token():
    """Get Bearer token dari Airflow Variable dengan auto-login & auto-refresh"""
    try:
        from utils.auth.echotik_auth import get_authenticated_token
        return get_authenticated_token()
    except Exception as e:
        logging.warning(f"Auto-auth service: {e}, mencoba fallback ke static Variable...")
        try:
            token = Variable.get('ECHOTIK_BEARER_TOKEN')
            if not token or token == 'PASTE_YOUR_TOKEN_HERE':
                raise ValueError("Bearer token belum di-set")
            return token
        except Exception as err:
            raise AirflowException(
                f"ECHOTIK_BEARER_TOKEN tidak dapat diperoleh: {err}\n"
                f"Pastikan Airflow Variable 'ECHOTIK_EMAIL' dan 'ECHOTIK_PASSWORD' sudah di-set."
            )


def _get_cookies():
    """Optional: cookies kalau Bearer saja tidak cukup"""
    try:
        return Variable.get('ECHOTIK_COOKIES', default_var='')
    except Exception:
        return ''


def _notify_token_error(notifier, error_msg: str = ""):
    """Kirim alert khusus ketika Bearer token invalid/expired."""
    msg = (
        "**BEARER TOKEN ERROR — ACTION REQUIRED**\n"
        f"Echotik API menolak request karena token tidak valid atau sudah **expired**.\n\n"
        f"**Error:** `{error_msg[:300] if error_msg else 'Authentication failed'}`\n\n"
        "**Cara fix:**\n"
        "1. Buka browser → login ke echotik.live\n"
        "2. F12 → Network → pilih request apapun → Headers → `Authorization`\n"
        "3. Copy nilai setelah `Bearer ` (format: `XXXXXXX|YYYY...`)\n"
        "4. Airflow UI → Admin → Variables → update `ECHOTIK_BEARER_TOKEN`\n"
        "5. Re-trigger DAG `echotik_data_collection`"
    )
    if notifier:
        notifier.send_message(msg)
    logging.error(f"[TOKEN ERROR] {error_msg}")


def _is_auth_error(error_msg: str) -> bool:
    """Cek apakah error berkaitan dengan auth/token (401, 403, unauthorized, dst)."""
    keywords = ['401', '403', 'authentication failed', 'unauthorized', 'forbidden',
                'token', 'bearer', 'expired']
    return any(kw in error_msg.lower() for kw in keywords)


def _build_client(notifier=None) -> EchotikAPIClient:
    """Build configured Echotik API client"""
    client = EchotikAPIClient(
        bearer_token=_get_bearer_token(),
        regional_headers=REGIONAL_HEADERS,
        cookies=_get_cookies(),
        timeout=PAGINATION_CONFIG['request_timeout_seconds'],
        max_retries=PAGINATION_CONFIG['max_retries'],
    )
    if notifier:
        client.set_notifier(notifier)
    return client


def _save_raw_json(data, filename: str):
    Path(RAW_JSON_PATH).mkdir(parents=True, exist_ok=True)
    filepath = Path(RAW_JSON_PATH) / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved: {filepath}")
    return str(filepath)


# ============================================
# TASK FUNCTIONS
# ============================================

def setup_dates(**context):
    """
    Calculate date ranges untuk 3 API yang granularity-nya beda:
    
    - Video Library: DAILY (start_time, end_time epoch)
    - Hashtag Library: WEEKLY (time_range YYYYMMDD-YYYYMMDD, 7 hari)
    - Video Selling: MONTHLY (time_range YYYYMMDD-YYYYMMDD, 1 bulan)
    
    Support manual override via DAG params (pop-up form saat trigger).
    """
    notifier = _get_notifier()
    
    # Get params dari context (manual override)
    params = context.get('params', {})
    p_library_start = (params.get('library_start_date') or '').strip()
    p_library_end = (params.get('library_end_date') or '').strip()
    p_hashtag_range = (params.get('hashtag_week_range') or '').strip()
    p_selling_range = (params.get('selling_month_range') or '').strip()
    
    now = datetime.utcnow()
    mode_flags = []
    
    library_start_dt = None
    library_end_dt = None
    library_start_epoch = None
    library_end_epoch = None
    
    # ============================================
    # 1. VIDEO LIBRARY (DAILY)
    # ============================================
    if p_library_start and p_library_end:
        if p_library_start.lower() == 'skip' or p_library_end.lower() == 'skip':
            mode_flags.append("Library: SKIP")
        else:
            # Manual mode
            try:
                library_start_dt = datetime.strptime(p_library_start, '%Y-%m-%d').replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                library_end_dt = datetime.strptime(p_library_end, '%Y-%m-%d').replace(
                    hour=23, minute=59, second=59, microsecond=0
                )
                mode_flags.append("Library: MANUAL")
                library_start_epoch = int(library_start_dt.timestamp())
                library_end_epoch = int(library_end_dt.timestamp())
            except Exception as e:
                logging.warning(f"Format library date salah ({e}), fallback ke auto")
                p_library_start = ''
    
    if not p_library_start:
        # Auto: kemarin
        yesterday = now - timedelta(days=1)
        library_start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        library_end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
        mode_flags.append("Library: AUTO (kemarin)")
        library_start_epoch = int(library_start_dt.timestamp())
        library_end_epoch = int(library_end_dt.timestamp())
    
    # ============================================
    # 2. HASHTAG (WEEKLY)
    # ============================================
    hashtag_time_range = None
    if p_hashtag_range:
        if p_hashtag_range.lower() == 'skip':
            hashtag_time_range = 'skip'
            mode_flags.append("Hashtag: SKIP")
        else:
            # Manual mode
            try:
                # Validasi format
                parts = p_hashtag_range.split('-')
                if len(parts) != 2 or len(parts[0]) != 8 or len(parts[1]) != 8:
                    raise ValueError("Format harus YYYYMMDD-YYYYMMDD")
                hashtag_time_range = p_hashtag_range
                mode_flags.append("Hashtag: MANUAL")
            except Exception as e:
                logging.warning(f"Format hashtag range salah ({e}), fallback ke auto")
                p_hashtag_range = ''
    
    if not p_hashtag_range:
        # Auto: minggu lalu (7 hari, Mon-Sun)
        # Hitung Senin minggu ini, lalu mundur 7 hari = Senin minggu lalu
        days_since_monday = now.weekday()  # 0=Monday, 6=Sunday
        this_monday = now - timedelta(days=days_since_monday)
        last_monday = this_monday - timedelta(days=7)
        last_sunday = last_monday + timedelta(days=6)
        
        hashtag_time_range = (
            f"{last_monday.strftime('%Y%m%d')}-"
            f"{last_sunday.strftime('%Y%m%d')}"
        )
        mode_flags.append("Hashtag: AUTO (minggu lalu)")
    
    # ============================================
    # 3. VIDEO SELLING (MONTHLY)
    # ============================================
    selling_time_range = None
    if p_selling_range:
        if p_selling_range.lower() == 'skip':
            selling_time_range = 'skip'
            mode_flags.append("Selling: SKIP")
        else:
            # Manual mode
            try:
                parts = p_selling_range.split('-')
                if len(parts) != 2 or len(parts[0]) != 8 or len(parts[1]) != 8:
                    raise ValueError("Format harus YYYYMMDD-YYYYMMDD")
                selling_time_range = p_selling_range
                mode_flags.append("Selling: MANUAL")
            except Exception as e:
                logging.warning(f"Format selling range salah ({e}), fallback ke auto")
                p_selling_range = ''
    
    if not p_selling_range:
        # Auto: bulan lalu
        first_day_this_month = now.replace(day=1)
        last_day_prev_month = first_day_this_month - timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        selling_time_range = (
            f"{first_day_prev_month.strftime('%Y%m%d')}-"
            f"{last_day_prev_month.strftime('%Y%m%d')}"
        )
        mode_flags.append("Selling: AUTO (bulan lalu)")
    
    # ============================================
    # FINAL DATES DICT
    # ============================================
    run_id = now.strftime('%Y%m%d_%H%M%S')
    
    # File suffix berdasarkan periode data
    suffix_parts = []
    if library_start_dt:
        suffix_parts.append(f"lib_{library_start_dt.strftime('%Y%m%d')}_to_{library_end_dt.strftime('%Y%m%d')}")
    if hashtag_time_range and hashtag_time_range != 'skip':
        suffix_parts.append(f"hash_{hashtag_time_range}")
    if selling_time_range and selling_time_range != 'skip':
        suffix_parts.append(f"sell_{selling_time_range}")
        
    file_suffix = "_".join(suffix_parts) if suffix_parts else "all_skipped"
    
    dates = {
        # Video Library (daily)
        'library_start_epoch': library_start_epoch,
        'library_end_epoch': library_end_epoch,
        'library_start_date': library_start_dt.strftime('%Y-%m-%d') if library_start_dt else 'skip',
        'library_end_date': library_end_dt.strftime('%Y-%m-%d') if library_end_dt else 'skip',
        
        # Hashtag (weekly)
        'hashtag_time_range': hashtag_time_range,
        
        # Video Selling (monthly)
        'selling_time_range': selling_time_range,
        
        # Metadata
        'run_id': run_id,
        'file_suffix': file_suffix,
        'modes': mode_flags,
    }
    
    logging.info(f"Date setup: {dates}")
    
    if notifier:
        modes_str = "\n".join(mode_flags)
        notifier.send_message(
            f"**Pipeline Started**\n"
            f"{modes_str}\n"
            f"Library: `{dates['library_start_date']}` to `{dates['library_end_date']}`\n"
            f"Hashtag week: `{dates['hashtag_time_range']}`\n"
            f"Selling month: `{dates['selling_time_range']}`\n"
            f"Pagination: page {PAGINATION_CONFIG['start_page']}-{PAGINATION_CONFIG['end_page']}, "
            f"per_page={PAGINATION_CONFIG['per_page']}\n"
            f"Run ID: `{run_id}`"
        )
    
    return dates

def validate_credentials(**context):
    """Validate Bearer token & cookies — termasuk test real API call ke Echotik & kirim alert status token."""
    notifier = _get_notifier()

    try:
        from utils.auth.echotik_auth import EchotikAuthenticator
        auth = EchotikAuthenticator(notifier=notifier)

        # Ambil token valid (akan kirim alert valid jika masih aktif, atau alert login jika diperbarui)
        bearer = auth.get_valid_token(force_refresh=False, send_alert_on_valid=True)
        cookies = _get_cookies()
        logging.info(f"Auth: bearer={len(bearer)} chars, cookies={len(cookies)} chars")

        # Test token dengan real API call (1 request, 1 record) — bukan sekadar cek Variable
        client = _build_client(notifier=notifier)
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        test_url = (
            f"https://echotik.live/api/v1/data/videos"
            f"?page=1&per_page=1"
            f"&start_time={int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())}"
            f"&end_time={int(yesterday.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())}"
            f"&sort=desc&order=views_count"
        )
        try:
            client.fetch_with_retry(url=test_url, page_num=1, referer_path="/data/videos")
            logging.info("Token validation: API responded OK ✅")
        except Exception as api_err:
            err_str = str(api_err)
            if _is_auth_error(err_str):
                _notify_token_error(notifier, err_str)
                raise AirflowException(
                    f"ECHOTIK_BEARER_TOKEN expired atau invalid: {err_str}\n"
                    "Cek kredensial ECHOTIK_EMAIL / ECHOTIK_PASSWORD di Airflow Variables."
                )
            # Error lain (timeout, network) — tetap raise tapi bukan token issue
            logging.warning(f"Token test request failed (non-auth): {api_err}")
            raise

        return {
            'status': 'valid',
            'has_bearer': True,
            'has_cookies': bool(cookies),
        }

    except AirflowException:
        raise
    except Exception as e:
        if notifier:
            notifier.send_task_failed('validate_credentials', str(e))
        raise


def fetch_video_library(**context):
    """Fetch video library page 1-7, per_page=20"""
    notifier = _get_notifier()
    start_time = time.time()
    
    if notifier:
        notifier.send_message(
            f"Fetching: **Video Library** ....."
            f"(page {PAGINATION_CONFIG['start_page']}-{PAGINATION_CONFIG['end_page']}, "
            f"per_page={PAGINATION_CONFIG['per_page']})"
        )
    
    dates = context['ti'].xcom_pull(task_ids='setup_dates')
    if dates.get('library_start_date') == 'skip':
        logging.info("Video Library fetch skipped by manual override")
        return {'records_count': 0, 'filepath': None, 'status': 'skipped'}
        
    client = _build_client(notifier=notifier)
    
    try:
        records = client.fetch_video_library(
            start_time=dates['library_start_epoch'],
            end_time=dates['library_end_epoch'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page'],
            delay_min=PAGINATION_CONFIG['delay_between_pages_min'],
            delay_max=PAGINATION_CONFIG['delay_between_pages_max'],
        )
        
        filename = f"video_library_{dates['library_start_date']}_to_{dates['library_end_date']}.json"
        filepath = _save_raw_json(records, filename)
        
        duration = time.time() - start_time
        pages_fetched = (len(records) + PAGINATION_CONFIG['per_page'] - 1) // PAGINATION_CONFIG['per_page']
        
        if notifier:
            notifier.send_category_summary(
                api_name='Video Library',
                category='All',
                total_records=len(records),
                pages_fetched=pages_fetched,
                duration_sec=duration,
                status='success'
            )
        
        # Throttle antar API
        delay = random.uniform(
            PAGINATION_CONFIG['delay_between_apis_min'],
            PAGINATION_CONFIG['delay_between_apis_max']
        )
        logging.info(f"Inter-API delay: {delay:.1f}s")
        time.sleep(delay)
        
        return {
            'records_count': len(records),
            'filepath': filepath,
            'status': 'success'
        }
    
    except Exception as e:
        err_str = str(e)
        logging.error(f"Failed: {err_str}")
        if _is_auth_error(err_str):
            _notify_token_error(notifier, err_str)
            raise AirflowException(f"Token expired atau invalid: {err_str}")
        elif notifier:
            notifier.send_task_failed('fetch_video_library', err_str)
        return {'records_count': 0, 'filepath': None, 'status': 'failed', 'error': err_str}


def fetch_hashtags(**context):
    """Fetch hashtags page 1-7, per_page=20"""
    notifier = _get_notifier()
    start_time = time.time()
    
    if notifier:
        notifier.send_message(
            f"Fetching: **Hashtag Library** .... "
            f"(page {PAGINATION_CONFIG['start_page']}-{PAGINATION_CONFIG['end_page']}, "
            f"per_page={PAGINATION_CONFIG['per_page']})"
        )
    
    dates = context['ti'].xcom_pull(task_ids='setup_dates')
    if dates.get('hashtag_time_range') == 'skip':
        logging.info("Hashtag Library fetch skipped by manual override")
        return {'records_count': 0, 'filepath': None, 'status': 'skipped'}
        
    client = _build_client(notifier=notifier)
    
    try:
        records = client.fetch_hashtags(
            time_range=dates['hashtag_time_range'],
            time_type=HASHTAG_CONFIG['time_type'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page'],
            delay_min=PAGINATION_CONFIG['delay_between_pages_min'],
            delay_max=PAGINATION_CONFIG['delay_between_pages_max'],
        )
        
        filename = f"hashtags_{dates['hashtag_time_range']}.json"
        filepath = _save_raw_json(records, filename)
        
        duration = time.time() - start_time
        pages_fetched = (len(records) + PAGINATION_CONFIG['per_page'] - 1) // PAGINATION_CONFIG['per_page']
        
        if notifier:
            notifier.send_category_summary(
                api_name='Hashtag Library',
                category='All',
                total_records=len(records),
                pages_fetched=pages_fetched,
                duration_sec=duration,
                status='success'
            )
        
        # Throttle antar API
        delay = random.uniform(
            PAGINATION_CONFIG['delay_between_apis_min'],
            PAGINATION_CONFIG['delay_between_apis_max']
        )
        time.sleep(delay)
        
        return {
            'records_count': len(records),
            'filepath': filepath,
            'status': 'success'
        }
    
    except Exception as e:
        err_str = str(e)
        logging.error(f"Failed: {err_str}")
        if _is_auth_error(err_str):
            _notify_token_error(notifier, err_str)
            raise AirflowException(f"Token expired atau invalid: {err_str}")
        elif notifier:
            notifier.send_task_failed('fetch_hashtags', err_str)
        return {'records_count': 0, 'filepath': None, 'status': 'failed', 'error': err_str}


def fetch_video_selling(**context):
    """Fetch video selling page 1-7, per_page=20"""
    notifier = _get_notifier()
    start_time = time.time()
    
    if notifier:
        notifier.send_message(
            f"Fetching: **Video Selling** "
            f"(page {PAGINATION_CONFIG['start_page']}-{PAGINATION_CONFIG['end_page']}, "
            f"per_page={PAGINATION_CONFIG['per_page']})"
        )
    
    dates = context['ti'].xcom_pull(task_ids='setup_dates')
    if dates.get('selling_time_range') == 'skip':
        logging.info("Video Selling fetch skipped by manual override")
        return {'records_count': 0, 'filepath': None, 'status': 'skipped'}
        
    client = _build_client(notifier=notifier)
    
    # Pilih time_range sesuai time_type
    time_type = VIDEO_SELLING_CONFIG['time_type']
    if time_type == 'monthly':
        time_range = dates['selling_time_range']
    else:  # daily
        time_range = dates['selling_time_range']
    
    try:
        records = client.fetch_video_selling(
            time_range=dates['selling_time_range'],   
            time_type=VIDEO_SELLING_CONFIG['time_type'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page'],
            delay_min=PAGINATION_CONFIG['delay_between_pages_min'],
            delay_max=PAGINATION_CONFIG['delay_between_pages_max'],
        )
        
        filename = f"video_selling_{dates['selling_time_range']}.json"
        filepath = _save_raw_json(records, filename)
        
        duration = time.time() - start_time
        pages_fetched = (len(records) + PAGINATION_CONFIG['per_page'] - 1) // PAGINATION_CONFIG['per_page']
        
        if notifier:
            notifier.send_category_summary(
                api_name='Video Selling',
                category='All',
                total_records=len(records),
                pages_fetched=pages_fetched,
                duration_sec=duration,
                status='success'
            )
        
        return {
            'records_count': len(records),
            'filepath': filepath,
            'status': 'success'
        }
    
    except Exception as e:
        err_str = str(e)
        logging.error(f"Failed: {err_str}")
        if _is_auth_error(err_str):
            _notify_token_error(notifier, err_str)
            raise AirflowException(f"Token expired atau invalid: {err_str}")
        elif notifier:
            notifier.send_task_failed('fetch_video_selling', err_str)
        return {'records_count': 0, 'filepath': None, 'status': 'failed', 'error': err_str}


def parser_and_validate(**context):
    """Parse semua raw JSON, validate, export Excel"""
    notifier = _get_notifier()
    ti = context['ti']
    dates = ti.xcom_pull(task_ids='setup_dates')
    
    if notifier:
        notifier.send_message("**Starting Parser & Validation...**")
    
    # Parse Video Library
    all_video_library = []
    library_result = ti.xcom_pull(task_ids='fetch_video_library')
    if library_result and library_result.get('filepath'):
        try:
            with open(library_result['filepath']) as f:
                raw_records = json.load(f)
            for raw in raw_records:
                parsed = parse_video_library_record(raw, 'All')
                parsed = validate_video_record(parsed)
                all_video_library.append(parsed)
            logging.info(f"Parsed {len(raw_records)} video library records")
        except Exception as e:
            logging.error(f"Failed to parse video library: {e}")
    
    # Parse Hashtags
    all_hashtags = []
    hashtag_result = ti.xcom_pull(task_ids='fetch_hashtags')
    if hashtag_result and hashtag_result.get('filepath'):
        try:
            with open(hashtag_result['filepath']) as f:
                raw_records = json.load(f)
            for raw in raw_records:
                parsed = parse_hashtag_record(raw)
                parsed = validate_hashtag_record(parsed)
                all_hashtags.append(parsed)
            logging.info(f"Parsed {len(raw_records)} hashtags")
        except Exception as e:
            logging.error(f"Failed to parse hashtags: {e}")
    
    # Parse Video Selling
    all_video_selling = []
    selling_result = ti.xcom_pull(task_ids='fetch_video_selling')
    if selling_result and selling_result.get('filepath'):
        try:
            with open(selling_result['filepath']) as f:
                raw_records = json.load(f)
            for raw in raw_records:
                parsed = parse_video_selling_record(raw, 'All')
                parsed = validate_video_record(parsed)
                all_video_selling.append(parsed)
            logging.info(f"Parsed {len(raw_records)} selling records")
        except Exception as e:
            logging.error(f"Failed to parse video selling: {e}")
    
    # Deduplicate
    all_video_library = deduplicate_records(all_video_library, 'echotik_video_id')
    all_hashtags = deduplicate_records(all_hashtags, 'echotik_tag_id')
    all_video_selling = deduplicate_records(all_video_selling, 'echotik_video_id')
    
    # Validation summary
    all_records = all_video_library + all_hashtags + all_video_selling
    summary = get_validation_summary(all_records)
    
    logging.info(f"Validation summary: {summary}")
    
    # Export Excel
    excel_path = export_to_excel(
        video_library_records=all_video_library,
        hashtag_records=all_hashtags,
        video_selling_records=all_video_selling,
        validation_summary=summary,
        output_dir=EXCEL_OUTPUT_PATH,
        run_date=dates['file_suffix']
    )
    
    if notifier:
        notifier.send_parser_summary(
            total_records=summary['total'],
            valid_records=summary['valid'],
            invalid_records=summary['invalid'],
            error_breakdown=summary['error_breakdown'],
            excel_filename=Path(excel_path).name
        )
    
    return {
        'excel_path': excel_path,
        'total_records': summary['total'],
        'valid_records': summary['valid'],
        'invalid_records': summary['invalid'],
    }


def dag_complete_summary(**context):
    """Final pipeline summary"""
    notifier = _get_notifier()
    ti = context['ti']
    
    parser_result = ti.xcom_pull(task_ids='parser_and_validate')
    dates = ti.xcom_pull(task_ids='setup_dates')
    
    dag_run = context['dag_run']
    duration = (datetime.utcnow() - dag_run.start_date.replace(tzinfo=None)).total_seconds()
    
    total_records = parser_result.get('total_records', 0) if parser_result else 0
    status = 'success' if total_records > 0 else 'partial'
    
    next_run = (datetime.utcnow() + timedelta(minutes=90)).strftime('%Y-%m-%d %H:%M UTC')
    
    if notifier:
        notifier.send_dag_summary(
            dag_id='echotik_data_collection',
            run_id=dates.get('run_id', 'unknown') if dates else 'unknown',
            status=status,
            duration_sec=duration,
            total_records=total_records,
            next_run=next_run
        )
    
    return {'status': status, 'total_records': total_records}


# ============================================
# DAG DEFINITION
# ============================================
with DAG(
    dag_id='echotik_data_collection',
    description='Echotik 3-API data collection (page 1-7, per_page 20, Bearer token auth)',
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=timedelta(hours=12),
    catchup=False,
    max_active_runs=1,
    tags=['echotik', 'data-collection', 'production'],
    params={
        "library_start_date": Param(
            "",
            type="string",
            title="Video Library — Start Date (Daily)",
            description="Format: YYYY-MM-DD. Kosongkan untuk auto (kemarin).",
        ),
        "library_end_date": Param(
            "",
            type="string",
            title="Video Library — End Date (Daily)",
            description="Format: YYYY-MM-DD. Kosongkan untuk auto (kemarin).",
        ),
        "hashtag_week_range": Param(
            "",
            type="string",
            title="Hashtag — Week Range (Weekly)",
            description="Format: YYYYMMDD-YYYYMMDD (7 hari). Contoh: 20260223-20260301. Kosongkan untuk auto (minggu lalu).",
        ),
        "selling_month_range": Param(
            "",
            type="string",
            title="Video Selling — Month Range (Monthly)",
            description="Format: YYYYMMDD-YYYYMMDD (1 bulan). Contoh: 20260401-20260430. Kosongkan untuk auto (bulan lalu).",
        ),
    },
) as dag:
    
    t_setup_dates = PythonOperator(
        task_id='setup_dates',
        python_callable=setup_dates,
    )
    
    t_validate_creds = PythonOperator(
        task_id='validate_credentials',
        python_callable=validate_credentials,
    )
    
    t_fetch_video_library = PythonOperator(
        task_id='fetch_video_library',
        python_callable=fetch_video_library,
    )
    
    t_fetch_hashtags = PythonOperator(
        task_id='fetch_hashtags',
        python_callable=fetch_hashtags,
    )
    
    t_fetch_video_selling = PythonOperator(
        task_id='fetch_video_selling',
        python_callable=fetch_video_selling,
    )
    
    t_parser = PythonOperator(
        task_id='parser_and_validate',
        python_callable=parser_and_validate,
        trigger_rule='one_success',
    )
    
    t_summary = PythonOperator(
        task_id='dag_complete_summary',
        python_callable=dag_complete_summary,
        trigger_rule='all_done',
    )
    
    # Flow: setup → validate → library → hashtags → selling → parser → summary
    # t_setup_dates >> t_parser >> t_summary
    t_setup_dates >> t_validate_creds >> t_fetch_video_library >> t_fetch_hashtags >> t_fetch_video_selling >> t_parser >> t_summary
