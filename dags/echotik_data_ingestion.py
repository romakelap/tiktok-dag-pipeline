"""
============================================================
ECHOTIK DATA INGESTION PIPELINE (DAG 2)
============================================================

DAG ini ingest data dari Excel (hasil DAG 1) ke MySQL database.

TRIGGER:
- Manual via Airflow UI
- Otomatis dari DAG 1 (via TriggerDagRunOperator)

REQUIREMENT Airflow Variables:
- DB_CONNECTION_STRING: SQLAlchemy connection string
  Format: mysql+pymysql://user:password@host:3306/tiktok_oltp
- DISCORD_WEBHOOK_URL: webhook Discord (sama dengan DAG 1)

FLOW:
1. scan_excel_files       → Cari file Excel di /raw-data/Excel/
2. validate_db_connection → Cek koneksi & tabel target ada
3. read_and_load_staging  → Baca Excel, bulk insert ke staging
4. validate_db_staging    → Validate di staging (dedup, NULL, business rules)
5. upsert_to_production   → UPSERT atomic ke production tables
6. refresh_bi_summary     → Recompute BI summary tables
7. audit_log              → INSERT audit log
8. move_processed_files   → Pindah Excel ke folder processed/
9. cleanup_staging        → Truncate staging
10. ingest_complete_summary → Final Discord notification
11. trigger_ml_inference  → Otomatis trigger DAG 3 (echotik_ml_daily_inference)

REQUIREMENT package (sudah included di docker-compose.yml):
- pandas, openpyxl, pymysql, sqlalchemy
"""
import sys
import os
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException, AirflowSkipException

sys.path.insert(0, '/opt/airflow/plugins')
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/config')

from hooks.db_hook import EchotikDBHook, get_db_hook
from utils.ingestion.excel_reader import (
    scan_excel_files,
    read_excel_sheets,
    add_ingest_metadata,
    standardize_video_library_columns,
    standardize_video_selling_columns,
    standardize_hashtag_columns,
    df_to_records,
)
from utils.ingestion.staging_loader import (
    load_videos_to_staging,
    load_hashtags_to_staging,
    load_snapshots_to_staging,
    build_snapshot_records_from_videos,
)
from utils.ingestion.db_validator import (
    validate_video_staging,
    validate_hashtag_staging,
)
from utils.ingestion.production_upsert import (
    upsert_regions,
    upsert_influencers,
    upsert_videos,
    upsert_hashtags,
    insert_video_metrics_snapshots,
    refresh_bi_video_summary,
    refresh_bi_hashtag_summary,
    upsert_products_and_links,
    upsert_video_categories,
)
from category_rules import CATEGORY_RULES
import ast
import re
import requests
import pandas as pd
from utils.monitoring.discord_notifier import DiscordNotifier
from db_config import (
    EXCEL_INPUT_PATH,
    EXCEL_PROCESSED_PATH,
    EXCEL_FAILED_PATH,
    EXCEL_FILE_PATTERN,
    PRODUCTION_TABLES,
    STAGING_TABLES,
    BATCH_SIZE,
    MOVE_PROCESSED_FILES,
    AUTO_REFRESH_BI,
    STAGING_CLEANUP,
)


default_args = {
    'owner': 'nico',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# ============================================
# HELPERS
# ============================================
def _get_notifier():
    try:
        webhook_url = Variable.get('DISCORD_WEBHOOK_URL')
        return DiscordNotifier(webhook_url)
    except Exception:
        return None


# ============================================
# TASK FUNCTIONS
# ============================================

def scan_excel_files_task(**context):
    """Step 1: Scan folder Excel untuk file yang belum di-ingest"""
    notifier = _get_notifier()
    
    files = scan_excel_files(
        directory=EXCEL_INPUT_PATH,
        pattern=EXCEL_FILE_PATTERN,
        exclude_subfolders=None
    )
    
    if not files:
        logging.warning("Tidak ada Excel file untuk di-ingest")
        if notifier:
            notifier.send_message(" **Ingestion Skipped**: Tidak ada Excel file untuk di-ingest")
        raise AirflowSkipException("No Excel files to ingest")
    
    run_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    if notifier:
        files_list = "\n".join([f"• `{Path(f).name}`" for f in files])
        notifier.send_message(
            f"**Ingestion Started**\n"
            f"Files to process: {len(files)}\n"
            f"{files_list}\n"
            f"Run ID: `{run_id}`"
        )
    
    return {
        'files': files,
        'run_id': run_id,
        'start_time': datetime.utcnow().isoformat(),
    }


def clean_and_tokenize(text_str):
    if not text_str or pd.isna(text_str):
        return []
    return re.findall(r"\w+", str(text_str).lower())


def classify_text(title, brief, hashtags=None):
    # Try calling ML category classifier service
    ml_url = "http://host.docker.internal:8001/ml/predict/category"
    try:
        t_val = str(title) if title and not pd.isna(title) else ""
        b_val = str(brief) if brief and not pd.isna(brief) else ""
        h_val = ""
        if hashtags:
            if isinstance(hashtags, list):
                h_val = " ".join(hashtags)
            elif isinstance(hashtags, str) and not pd.isna(hashtags):
                h_val = hashtags
        
        payload = {
            "title": t_val,
            "hashtags": h_val,
            "nlp_text": b_val
        }
        
        response = requests.post(ml_url, json=payload, timeout=2.0)
        if response.status_code == 200:
            res_data = response.json()
            if res_data.get("success") and "data" in res_data:
                category = res_data["data"].get("category")
                if category:
                    return category
    except Exception as e:
        pass

    # Initialize scores as fallback
    scores = {cat: 0.0 for cat in CATEGORY_RULES}
    
    title_words = clean_and_tokenize(title)
    brief_words = clean_and_tokenize(brief)
    
    # Process hashtags if present
    hashtag_words = []
    if hashtags and not pd.isna(hashtags):
        if isinstance(hashtags, list):
            for h in hashtags:
                hashtag_words.extend(clean_and_tokenize(str(h)))
        else:
            hashtag_words = clean_and_tokenize(str(hashtags))
    
    for category, rule in CATEGORY_RULES.items():
        cat_keywords = set(rule.get("keywords", []))
        
        # Match keywords in title_full (+1.5)
        for word in title_words:
            if word in cat_keywords:
                scores[category] += 1.5
                
        # Match keywords in title_brief (+1.0)
        for word in brief_words:
            if word in cat_keywords:
                scores[category] += 1.0
                
        # Match keywords in hashtags (+2.0)
        for word in hashtag_words:
            if word in cat_keywords:
                scores[category] += 2.0
                
    # Find category with highest score
    max_cat = max(scores, key=scores.get)
    max_score = scores[max_cat]
    
    if max_score == 0:
        # Balanced deterministic hash fallback across the 5 core categories
        core_categories = ["Edukasi", "Komedi", "Kuliner", "Lifestyle & Home", "Teknologi"]
        combined_seed = f"{title}_{brief}"
        return core_categories[abs(hash(combined_seed)) % len(core_categories)]
    
    return max_cat


def tagging_category_name_task(**context):
    """
    Step 2.5: Tag categories in Excel sheets.
    - Video Library: Tag category_name using title_full & title_brief
    - Video Selling: Tag category_name using title_full & title_brief
    - Hashtags: Map category_name based on video_library.extracted_hashtags mapping
    Save changes in-place to the Excel files in the processed folder.
    """
    notifier = _get_notifier()
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    files = scan_result['files']
    
    for filepath in files:
        logging.info(f"Tagging categories in: {Path(filepath).name}")
        
        try:
            excel_file = pd.ExcelFile(filepath, engine='openpyxl')
            available_sheets = excel_file.sheet_names
            
            dtypes = {
                'influencer_id': str,
                'echotik_video_id': str,
                'echotik_tag_id': str,
            }
            df_lib = excel_file.parse('video_library', dtype=dtypes) if 'video_library' in available_sheets else pd.DataFrame()
            df_sell = excel_file.parse('video_selling', dtype=dtypes) if 'video_selling' in available_sheets else pd.DataFrame()
            
            hashtag_sheet_name = 'hashtags'
            if 'hashtags' in available_sheets:
                hashtag_sheet_name = 'hashtags'
            elif 'Hashtag' in available_sheets:
                hashtag_sheet_name = 'Hashtag'
            elif 'hashtag' in available_sheets:
                hashtag_sheet_name = 'hashtag'
                
            df_tags = excel_file.parse(hashtag_sheet_name, dtype=dtypes) if hashtag_sheet_name in available_sheets else pd.DataFrame()
            
            # Keep other sheets (like validation_report)
            other_sheets = {}
            for sheet in available_sheets:
                if sheet not in ['video_library', 'video_selling', hashtag_sheet_name]:
                    other_sheets[sheet] = excel_file.parse(sheet)
            
            excel_file.close()
            
            # --- 1. Tag video_library ---
            hashtag_to_category_counts = {}
            
            if not df_lib.empty:
                # Ensure columns exist
                for col in ['title_full', 'title_brief', 'category_name', 'extracted_hashtags']:
                    if col not in df_lib.columns:
                        df_lib[col] = None
                        
                for idx, row in df_lib.iterrows():
                    title = row.get('title_full', '')
                    brief = row.get('title_brief', '')
                    raw_tags = row.get('extracted_hashtags', '')
                    category = classify_text(title, brief, hashtags=raw_tags)
                    df_lib.at[idx, 'category_name'] = category
                    
                    # Track hashtags to category mapping
                    raw_tags = row.get('extracted_hashtags', '')
                    tags_list = []
                    if raw_tags and not pd.isna(raw_tags):
                        try:
                            if isinstance(raw_tags, str):
                                if raw_tags.startswith('['):
                                    tags_list = ast.literal_eval(raw_tags)
                                else:
                                    tags_list = [t.strip() for t in raw_tags.split(',') if t.strip()]
                            elif isinstance(raw_tags, list):
                                tags_list = raw_tags
                        except Exception as e:
                            logging.warning(f"Error parsing extracted_hashtags: {raw_tags}, error: {e}")
                            
                    for tag in tags_list:
                        tag_clean = str(tag).strip().lower()
                        if not tag_clean:
                            continue
                        if tag_clean not in hashtag_to_category_counts:
                            hashtag_to_category_counts[tag_clean] = {}
                        hashtag_to_category_counts[tag_clean][category] = hashtag_to_category_counts[tag_clean].get(category, 0) + 1
            
            # --- 2. Tag video_selling ---
            if not df_sell.empty:
                # Ensure columns exist
                for col in ['title_full', 'title_brief', 'category_name']:
                    if col not in df_sell.columns:
                        df_sell[col] = None
                        
                for idx, row in df_sell.iterrows():
                    title = row.get('title_full', '')
                    brief = row.get('title_brief', '')
                    category = classify_text(title, brief)
                    df_sell.at[idx, 'category_name'] = category
                    
            # --- 3. Tag hashtags sheet ---
            hashtag_to_category = {}
            for tag, counts in hashtag_to_category_counts.items():
                best_cat = max(counts, key=counts.get)
                hashtag_to_category[tag] = best_cat
                
            if not df_tags.empty:
                if 'category_name' not in df_tags.columns:
                    df_tags['category_name'] = None
                    
                for idx, row in df_tags.iterrows():
                    tag_title = str(row.get('tag_title', row.get('hashtag', row.get('name', '')))).strip().lower()
                    if tag_title.startswith('#'):
                        tag_title = tag_title[1:]
                    category = hashtag_to_category.get(tag_title)
                    if not category:
                        category = classify_text(tag_title, tag_title)
                    df_tags.at[idx, 'category_name'] = category
            
            # --- 4. Write back to Excel file in-place ---
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                if not df_lib.empty:
                    df_lib.to_excel(writer, sheet_name='video_library', index=False)
                if not df_tags.empty:
                    df_tags.to_excel(writer, sheet_name=hashtag_sheet_name, index=False)
                if not df_sell.empty:
                    df_sell.to_excel(writer, sheet_name='video_selling', index=False)
                
                # Write back other sheets
                for sheet_name, df_other in other_sheets.items():
                    df_other.to_excel(writer, sheet_name=sheet_name, index=False)
                    
            logging.info(f"Successfully tagged categories for {Path(filepath).name}")
            
        except Exception as e:
            logging.error(f"Error during category tagging of {filepath}: {e}")
            if notifier:
                notifier.send_message(f"⚠️ **Tagging Failed** for `{Path(filepath).name}`: {e}")
            raise

    return {'tagged_files': len(files)}


def validate_db_connection_task(**context):
    """Step 2: Validate DB connection & tables exist"""
    notifier = _get_notifier()
    
    try:
        db = get_db_hook()
        
        # Test connection
        if not db.test_connection():
            raise Exception("DB connection test failed")
        
        # Check production tables exist
        prod_tables = list(PRODUCTION_TABLES.values())
        check = db.check_tables_exist(prod_tables)
        
        missing = [t for t, exists in check.items() if not exists]
        if missing:
            raise Exception(f"Tables missing: {missing}. Run schema SQL setup dulu!")
        
        # Check staging tables exist
        staging_tables = list(STAGING_TABLES.values())
        check_staging = db.check_tables_exist(staging_tables)
        missing_staging = [t for t, exists in check_staging.items() if not exists]
        if missing_staging:
            raise Exception(f"Staging tables missing: {missing_staging}. Run sql/01_ingestion_schema.sql!")
        
        # Check audit log table
        audit_check = db.check_tables_exist(['ingest_audit_log'])
        if not audit_check['ingest_audit_log']:
            raise Exception("Table ingest_audit_log missing. Run sql/01_ingestion_schema.sql!")
        
        db.close()
        
        logging.info("DB connection & schema validated")
        return {'status': 'valid'}
    
    except Exception as e:
        if notifier:
            notifier.send_task_failed('validate_db_connection', str(e))
        raise


def read_and_load_staging_task(**context):
    """
    Step 3: Read Excel files, transform, load ke staging tables.
    
    Per file:
    - Read 3 sheets
    - Standardize columns
    - Add metadata (run_id, source_file)
    - Build snapshot records
    - Bulk insert ke staging
    """
    notifier = _get_notifier()
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    files = scan_result['files']
    run_id = scan_result['run_id']
    
    db = get_db_hook()
    
    # Aggregator untuk semua file
    all_videos = []
    all_hashtags = []
    all_snapshots = []
    all_selling_records = []
    file_summary = []
    
    try:
        for filepath in files:
            try:
                logging.info(f"Processing: {Path(filepath).name}")
                
                # Read all sheets
                sheets = read_excel_sheets(filepath, sheets_to_read=[
                    'video_library', 'hashtags', 'video_selling'
                ])
                
                # ===== Process video_library =====
                df_library = sheets.get('video_library', None)
                videos_lib_count = 0
                if df_library is not None and not df_library.empty:
                    df_library = standardize_video_library_columns(df_library)
                    df_library = add_ingest_metadata(df_library, run_id, filepath)
                    library_records = df_to_records(df_library)
                    all_videos.extend(library_records)
                    videos_lib_count = len(library_records)
                
                # ===== Process video_selling =====
                df_selling = sheets.get('video_selling', None)
                videos_sell_count = 0
                if df_selling is not None and not df_selling.empty:
                    df_selling = standardize_video_selling_columns(df_selling)
                    df_selling = add_ingest_metadata(df_selling, run_id, filepath)
                    selling_records = df_to_records(df_selling)
                    all_videos.extend(selling_records)
                    for r in selling_records:
                        all_selling_records.append({
                            'echotik_video_id': r.get('echotik_video_id'),
                            'products_json': r.get('products_json')
                        })
                    videos_sell_count = len(selling_records)
                
                # ===== Process hashtags =====
                df_hashtags = sheets.get('hashtags', None)
                hashtags_count = 0
                if df_hashtags is not None and not df_hashtags.empty:
                    df_hashtags = standardize_hashtag_columns(df_hashtags)
                    df_hashtags = add_ingest_metadata(df_hashtags, run_id, filepath)
                    hashtag_records = df_to_records(df_hashtags)
                    all_hashtags.extend(hashtag_records)
                    hashtags_count = len(hashtag_records)
                
                file_summary.append({
                    'file': Path(filepath).name,
                    'library': videos_lib_count,
                    'selling': videos_sell_count,
                    'hashtags': hashtags_count,
                    'status': 'parsed'
                })
                
            except Exception as e:
                logging.error(f"Failed to process {filepath}: {e}")
                file_summary.append({
                    'file': Path(filepath).name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        # Build snapshots dari semua videos
        all_snapshots = build_snapshot_records_from_videos(all_videos)
        
        logging.info(f"Total: videos={len(all_videos)}, hashtags={len(all_hashtags)}, snapshots={len(all_snapshots)}")
        
        # ===== Bulk insert ke staging =====
        videos_inserted = load_videos_to_staging(
            db, all_videos, batch_size=BATCH_SIZE, truncate_first=True
        )
        hashtags_inserted = load_hashtags_to_staging(
            db, all_hashtags, batch_size=BATCH_SIZE, truncate_first=True
        )
        snapshots_inserted = load_snapshots_to_staging(
            db, all_snapshots, batch_size=BATCH_SIZE, truncate_first=True
        )
        
        db.close()
        
        if notifier:
            notifier.send_message(
                f"**Staging Loaded**\n"
                f"Files processed: {len(files)}\n"
                f"Videos: {videos_inserted}\n"
                f"Hashtags: {hashtags_inserted}\n"
                f"Snapshots: {snapshots_inserted}"
            )
        
        return {
            'files_processed': len(files),
            'videos_inserted': videos_inserted,
            'hashtags_inserted': hashtags_inserted,
            'snapshots_inserted': snapshots_inserted,
            'file_summary': file_summary,
            'selling_records': all_selling_records,
        }
    
    except Exception as e:
        db.close()
        if notifier:
            notifier.send_task_failed('read_and_load_staging', str(e))
        raise


def validate_db_staging_task(**context):
    """Step 4: Validate data di staging (dedup, NULL, business rules)"""
    notifier = _get_notifier()
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    run_id = scan_result['run_id']
    
    db = get_db_hook()
    
    try:
        video_summary = validate_video_staging(db, run_id)
        hashtag_summary = validate_hashtag_staging(db, run_id)
        
        total_valid = video_summary['valid'] + hashtag_summary['valid']
        total_invalid = video_summary['invalid'] + hashtag_summary['invalid']
        
        db.close()
        
        if notifier:
            error_lines = ""
            if video_summary.get('error_breakdown'):
                error_lines = "\n".join([
                    f"  • {k}: {v}" 
                    for k, v in video_summary['error_breakdown'].items()
                ])
            
            notifier.send_message(
                f"**DB Validation Complete**\n"
                f"Videos: {video_summary['valid']} valid, {video_summary['invalid']} invalid\n"
                f"Hashtags: {hashtag_summary['valid']} valid, {hashtag_summary['invalid']} invalid\n"
                + (f"\n**Errors:**\n{error_lines}" if error_lines else "")
            )
        
        return {
            'video_summary': video_summary,
            'hashtag_summary': hashtag_summary,
            'total_valid': total_valid,
            'total_invalid': total_invalid,
        }
    
    except Exception as e:
        db.close()
        if notifier:
            notifier.send_task_failed('validate_db_staging', str(e))
        raise


def upsert_to_production_task(**context):
    """Step 5: Atomic UPSERT dari staging ke production tables"""
    notifier = _get_notifier()
    ti = context['ti']
    
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    run_id = scan_result['run_id']
    
    staging_result = ti.xcom_pull(task_ids='read_and_load_staging')  # TAMBAH
    selling_records = staging_result.get('selling_records', [])       # TAMBAH
    
    db = get_db_hook()
    
    try:
        # Order: regions → influencers → videos → hashtags → snapshots (FK dependency)
        regions_result = upsert_regions(db, run_id)
        influencers_result = upsert_influencers(db, run_id)
        videos_result = upsert_videos(db, run_id)
        categories_result = upsert_video_categories(db, run_id)
        hashtags_result = upsert_hashtags(db, run_id)
        snapshots_result = insert_video_metrics_snapshots(db, run_id)
        products_result = upsert_products_and_links(db, run_id, selling_records)  # TAMBAH
        
        db.close()
        
        if notifier:
            notifier.send_message(
                f"**Production UPSERT Complete**\n"
                f"Regions: {regions_result['inserted']} new\n"
                f"Influencers: {influencers_result['inserted']} ins, {influencers_result['updated']} upd\n"
                f"Videos: {videos_result['inserted']} ins, {videos_result['updated']} upd\n"
                f"Categories: {categories_result['inserted']} tagged\n"
                f"Hashtags: {hashtags_result['inserted']} ins, {hashtags_result['updated']} upd\n"
                f"Snapshots: {snapshots_result['inserted']} appended (time-series)\n"
                f"Products: {products_result['products']} upserted\n"           # TAMBAH
                f"Video-Products: {products_result['video_products']} linked\n" # TAMBAH
            )
        
        return {
            'regions': regions_result,
            'influencers': influencers_result,
            'videos': videos_result,
            'categories': categories_result,
            'hashtags': hashtags_result,
            'snapshots': snapshots_result,
            'products': products_result,  # TAMBAH
        }
    
    except Exception as e:
        db.close()
        if notifier:
            notifier.send_task_failed('upsert_to_production', str(e))
        raise


def refresh_bi_summary_task(**context):
    """Step 6: Refresh BI summary tables (precomputed aggregates)"""
    notifier = _get_notifier()
    
    if not AUTO_REFRESH_BI:
        logging.info("AUTO_REFRESH_BI = False, skipping")
        return {'skipped': True}
    
    db = get_db_hook()
    
    try:
        video_count = refresh_bi_video_summary(db)
        hashtag_count = refresh_bi_hashtag_summary(db)
        
        db.close()
        
        if notifier:
            notifier.send_message(
                f"**BI Summary Refreshed**\n"
                f"bi_video_revenue_summary: {video_count} rows\n"
                f"bi_hashtag_revenue_summary: {hashtag_count} rows"
            )
        
        return {'video_summary': video_count, 'hashtag_summary': hashtag_count}
    
    except Exception as e:
        db.close()
        logging.warning(f"BI refresh failed (non-fatal): {e}")
        return {'error': str(e)}


def audit_log_task(**context):
    """Step 7: INSERT audit log"""
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    staging_result = ti.xcom_pull(task_ids='read_and_load_staging')
    validate_result = ti.xcom_pull(task_ids='validate_db_staging')
    upsert_result = ti.xcom_pull(task_ids='upsert_to_production')
    
    run_id = scan_result['run_id']
    start_time = scan_result['start_time']
    end_time = datetime.utcnow()
    duration = int((end_time - datetime.fromisoformat(start_time)).total_seconds())
    
    db = get_db_hook()
    
    try:
        files = scan_result.get('files', [])
        source_files = ", ".join([Path(f).name for f in files])[:255]
        
        db.execute("""
            INSERT INTO ingest_audit_log (
                run_id, source_file,
                start_time, end_time, duration_sec,
                total_records_read, valid_records, invalid_records,
                videos_inserted, videos_updated,
                hashtags_inserted, hashtags_updated,
                snapshots_inserted,
                influencers_inserted, influencers_updated,
                status
            ) VALUES (
                :run_id, :source_file,
                :start_time, :end_time, :duration,
                :total, :valid, :invalid,
                :v_ins, :v_upd,
                :h_ins, :h_upd,
                :s_ins,
                :i_ins, :i_upd,
                :status
            )
        """, params={
            'run_id': run_id,
            'source_file': source_files,
            'start_time': start_time,
            'end_time': end_time.isoformat(),
            'duration': duration,
            'total': (staging_result['videos_inserted'] or 0) + (staging_result['hashtags_inserted'] or 0),
            'valid': validate_result['total_valid'],
            'invalid': validate_result['total_invalid'],
            'v_ins': upsert_result['videos']['inserted'],
            'v_upd': upsert_result['videos']['updated'],
            'h_ins': upsert_result['hashtags']['inserted'],
            'h_upd': upsert_result['hashtags']['updated'],
            's_ins': upsert_result['snapshots']['inserted'],
            'i_ins': upsert_result['influencers']['inserted'],
            'i_upd': upsert_result['influencers']['updated'],
            'status': 'SUCCESS',
        })
        
        db.close()
        logging.info(f"Audit log saved for run {run_id}")
        return {'audit_logged': True}
    
    except Exception as e:
        db.close()
        logging.error(f"Audit log failed: {e}")
        return {'error': str(e)}


def move_processed_files_task(**context):
    """Step 8: Pindah Excel files ke folder processed/"""
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    files = scan_result.get('files', [])
    
    if not MOVE_PROCESSED_FILES:
        logging.info("MOVE_PROCESSED_FILES = False, skipping")
        return {'skipped': True}
    
    processed_dir = Path(EXCEL_PROCESSED_PATH)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    moved = []
    failed = []
    
    for filepath in files:
        try:
            src = Path(filepath)
            dst = processed_dir / src.name
            
            # Skip if source and destination are the same path
            if src.resolve() == dst.resolve():
                logging.info(f"File {src.name} is already in the processed folder, skipping move.")
                moved.append(str(dst))
                continue
                
            # Kalau file dengan nama sama sudah ada di processed/, append timestamp
            if dst.exists():
                stem = src.stem
                suffix = src.suffix
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                dst = processed_dir / f"{stem}_ingested{timestamp}{suffix}"
            
            shutil.move(str(src), str(dst))
            moved.append(str(dst))
            logging.info(f"Moved: {src.name} → processed/{dst.name}")
        
        except Exception as e:
            logging.error(f"Failed to move {filepath}: {e}")
            failed.append({'file': filepath, 'error': str(e)})
    
    return {'moved': len(moved), 'failed': len(failed)}


def cleanup_staging_task(**context):
    """Step 9: Cleanup staging tables"""
    if STAGING_CLEANUP == 'keep_forever':
        logging.info("STAGING_CLEANUP = keep_forever, skipping")
        return {'skipped': True}
    
    db = get_db_hook()
    
    try:
        if STAGING_CLEANUP == 'truncate_immediately':
            for staging_table in STAGING_TABLES.values():
                db.truncate_table(staging_table)
            logging.info("All staging tables truncated")
        
        elif STAGING_CLEANUP == 'keep_24h':
            # Delete rows > 24 jam
            for staging_table in STAGING_TABLES.values():
                db.execute(f"""
                    DELETE FROM {staging_table}
                    WHERE ingested_at < NOW() - INTERVAL 1 DAY
                """)
            logging.info("Staging cleanup: removed rows > 24h")
        
        db.close()
        return {'cleanup_mode': STAGING_CLEANUP}
    
    except Exception as e:
        db.close()
        logging.warning(f"Staging cleanup failed (non-fatal): {e}")
        return {'error': str(e)}


def ingest_complete_summary_task(**context):
    """Step 10: Final Discord notification"""
    notifier = _get_notifier()
    ti = context['ti']
    
    scan_result = ti.xcom_pull(task_ids='scan_excel_files')
    staging_result = ti.xcom_pull(task_ids='read_and_load_staging')
    validate_result = ti.xcom_pull(task_ids='validate_db_staging')
    upsert_result = ti.xcom_pull(task_ids='upsert_to_production')
    move_result = ti.xcom_pull(task_ids='move_processed_files')
    
    run_id = scan_result['run_id']
    start_time = datetime.fromisoformat(scan_result['start_time'])
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    total_to_db = (
        (upsert_result['videos']['inserted'] or 0) +
        (upsert_result['videos']['updated'] or 0) +
        (upsert_result['categories']['inserted'] or 0) +
        (upsert_result['hashtags']['inserted'] or 0) +
        (upsert_result['hashtags']['updated'] or 0) +
        (upsert_result['snapshots']['inserted'] or 0)
    )
    
    if notifier:
        notifier.send_dag_summary(
            dag_id='echotik_data_ingestion',
            run_id=run_id,
            status='success',
            duration_sec=duration,
            total_records=total_to_db,
            next_run=None
        )
        
        notifier.send_message(
            f"**Ingestion Complete!**\n"
            f"Files processed: {scan_result.get('files', [])}\n"
            f"Total to DB: {total_to_db} records\n"
            f"   • Videos: {upsert_result['videos']['inserted']} ins, {upsert_result['videos']['updated']} upd\n"
            f"   • Categories: {upsert_result['categories']['inserted']} tagged\n"
            f"   • Hashtags: {upsert_result['hashtags']['inserted']} ins, {upsert_result['hashtags']['updated']} upd\n"
            f"   • Snapshots: {upsert_result['snapshots']['inserted']} new\n"
            f"   • Influencers: {upsert_result['influencers']['inserted']} ins, {upsert_result['influencers']['updated']} upd\n"
            f"Files moved: {move_result.get('moved', 0)}\n"
            f"Duration: {duration/60:.1f} min\n"
            f"DB is Ready!!"
        )
    
    return {'status': 'success'}


# ============================================
# DAG DEFINITION
# ============================================
with DAG(
    dag_id='echotik_data_ingestion',
    description='Ingest Excel data ke MySQL (production DB)',
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,  # Manual trigger only (atau dari DAG 1)
    catchup=False,
    max_active_runs=1,
    tags=['echotik', 'ingestion', 'database'],
) as dag:
    
    t_scan = PythonOperator(
        task_id='scan_excel_files',
        python_callable=scan_excel_files_task,
    )
    
    t_validate_db = PythonOperator(
        task_id='validate_db_connection',
        python_callable=validate_db_connection_task,
    )
    
    t_load_staging = PythonOperator(
        task_id='read_and_load_staging',
        python_callable=read_and_load_staging_task,
    )
    
    t_tag_categories = PythonOperator(
        task_id='tagging_category_name',
        python_callable=tagging_category_name_task,
    )
    
    t_validate_staging = PythonOperator(
        task_id='validate_db_staging',
        python_callable=validate_db_staging_task,
    )
    
    t_upsert = PythonOperator(
        task_id='upsert_to_production',
        python_callable=upsert_to_production_task,
    )
    
    t_refresh_bi = PythonOperator(
        task_id='refresh_bi_summary',
        python_callable=refresh_bi_summary_task,
        trigger_rule='all_success',
    )
    
    t_audit = PythonOperator(
        task_id='audit_log',
        python_callable=audit_log_task,
        trigger_rule='all_success',
    )
    
    t_move = PythonOperator(
        task_id='move_processed_files',
        python_callable=move_processed_files_task,
        trigger_rule='all_success',
    )
    
    t_cleanup = PythonOperator(
        task_id='cleanup_staging',
        python_callable=cleanup_staging_task,
        trigger_rule='all_success',
    )
    
    t_summary = PythonOperator(
        task_id='ingest_complete_summary',
        python_callable=ingest_complete_summary_task,
        trigger_rule='all_success',
    )

    # Trigger DAG 3 (ML inference) otomatis setelah ingestion selesai.
    # trigger_rule='all_done' agar tetap jalan meski ada task upstream yang skip/gagal,
    # selama t_upsert dan t_refresh_bi berhasil.
    t_trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_inference',
        trigger_dag_id='echotik_ml_daily_inference',
        wait_for_completion=False,   # DAG 2 tidak nunggu DAG 3 selesai
        reset_dag_run=True,          # Jika DAG 3 sudah jalan hari ini, reset & jalankan ulang
        trigger_rule='all_success',  # Hanya trigger jika t_summary sukses
    )

    # FLOW
    t_scan >> t_validate_db >> t_tag_categories >> t_load_staging >> t_validate_staging >> t_upsert
    t_upsert >> t_move
    t_upsert >> [t_refresh_bi, t_audit]
    [t_move, t_refresh_bi, t_audit] >> t_cleanup >> t_summary >> t_trigger_ml

