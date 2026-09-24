#!/usr/bin/env python3
"""
============================================================
ECHOTIK SERVERLESS PIPELINE ORCHESTRATOR
============================================================
Script ini menggabungkan alur kerja:
1. Data Collection (Echotik API) - Sebelumnya DAG 1
2. Data Ingestion (MySQL DB) - Sebelumnya DAG 2

Cocok dijalankan di GitHub Actions / serverless runner.
Mengambil parameter sensitif dari Environment Variables.
"""

import os
import sys
import json
import time
import random
import logging
import re
import ast
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from typing import Optional

# ─── Setup Python Path ──────────────────────────────────────
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'plugins'))
sys.path.insert(0, str(project_root / 'config'))

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# ─── Import Project Modules ─────────────────────────────────
from hooks.echotik_client import EchotikAPIClient, EchotikAuthException
from hooks.db_hook import EchotikDBHook
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

# Import Configs
from categories_config import (
    VIDEO_LIBRARY_CONFIG,
    HASHTAG_CONFIG,
    VIDEO_SELLING_CONFIG,
    PAGINATION_CONFIG,
    REGIONAL_HEADERS,
)
from db_config import (
    PRODUCTION_TABLES,
    STAGING_TABLES,
    BATCH_SIZE,
    AUTO_REFRESH_BI,
    STAGING_CLEANUP,
)
from category_rules import CATEGORY_RULES

# Import Ingestion steps
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

# ─── Dynamic Directory Setup ────────────────────────────────
RAW_JSON_PATH = Path(project_root) / "raw-data" / "json"
EXCEL_OUTPUT_PATH = Path(project_root) / "raw-data" / "Excel"

RAW_JSON_PATH.mkdir(parents=True, exist_ok=True)
EXCEL_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# ─── Helpers ────────────────────────────────────────────────
def get_env_or_raise(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        logging.error(f"Missing environment variable: {key}")
        raise ValueError(f"Missing required environment variable: {key}")
    return val


def get_notifier() -> Optional[DiscordNotifier]:
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    if webhook_url and webhook_url.strip():
        return DiscordNotifier(webhook_url)
    return None


def clean_and_tokenize(text_str):
    if not text_str or pd.isna(text_str):
        return []
    return re.findall(r"\w+", str(text_str).lower())


def classify_text(title, brief, hashtags=None):
    # Rule-Based Fallback as primary when ML service is offline
    scores = {cat: 0.0 for cat in CATEGORY_RULES}
    
    title_words = clean_and_tokenize(title)
    brief_words = clean_and_tokenize(brief)
    
    for category, rule in CATEGORY_RULES.items():
        # Match keywords in title_full
        for word in title_words:
            if word in rule["keywords"]:
                scores[category] += 1.5
                
        # Match keywords in title_brief
        for word in brief_words:
            if word in rule["keywords"]:
                scores[category] += 1.0
                
    max_cat = max(scores, key=scores.get)
    max_score = scores[max_cat]
    
    if max_score == 0:
        return "Lifestyle & Home"
    
    return max_cat


def setup_dates():
    """Menghitung range tanggal untuk data collection"""
    now = datetime.utcnow()
    
    # 1. Video Library (Daily - Kemarin)
    yesterday = now - timedelta(days=1)
    library_start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    library_end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    library_start_epoch = int(library_start_dt.timestamp())
    library_end_epoch = int(library_end_dt.timestamp())
    
    # 2. Hashtag (Weekly - Minggu Lalu)
    days_since_monday = now.weekday()
    this_monday = now - timedelta(days=days_since_monday)
    last_monday = this_monday - timedelta(days=7)
    last_sunday = last_monday + timedelta(days=6)
    hashtag_time_range = f"{last_monday.strftime('%Y%m%d')}-{last_sunday.strftime('%Y%m%d')}"
    
    # 3. Video Selling (Monthly - Bulan Lalu)
    first_day_this_month = now.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    selling_time_range = f"{first_day_prev_month.strftime('%Y%m%d')}-{last_day_prev_month.strftime('%Y%m%d')}"
    
    run_id = now.strftime('%Y%m%d_%H%M%S')
    file_suffix = f"lib_{library_start_dt.strftime('%Y%m%d')}_to_{library_end_dt.strftime('%Y%m%d')}_hash_{hashtag_time_range}_sell_{selling_time_range}"
    
    return {
        'library_start_epoch': library_start_epoch,
        'library_end_epoch': library_end_epoch,
        'library_start_date': library_start_dt.strftime('%Y-%m-%d'),
        'library_end_date': library_end_dt.strftime('%Y-%m-%d'),
        'hashtag_time_range': hashtag_time_range,
        'selling_time_range': selling_time_range,
        'run_id': run_id,
        'file_suffix': file_suffix
    }


def save_raw_json(data, filename: str) -> str:
    filepath = RAW_JSON_PATH / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"Saved JSON: {filepath}")
    return str(filepath)


# ─── MAIN PIPELINE RUNNER ───────────────────────────────────
def main():
    logging.info("Starting Echotik Serverless Pipeline...")
    
    # Check Environment Variables
    try:
        bearer_token = get_env_or_raise('ECHOTIK_BEARER_TOKEN')
        db_conn_string = get_env_or_raise('DB_CONNECTION_STRING')
    except ValueError as e:
        sys.exit(1)
        
    notifier = get_notifier()
    dates = setup_dates()
    run_id = dates['run_id']
    file_suffix = dates['file_suffix']
    excel_filename = f"echotik_data_{file_suffix}.xlsx"
    excel_path = EXCEL_OUTPUT_PATH / excel_filename
    
    logging.info(f"Run ID: {run_id}")
    logging.info(f"Target Excel: {excel_path}")
    
    if notifier:
        notifier.send_message(
            f"🚀 **Pipeline Started (GitHub Actions)**\n"
            f"Run ID: `{run_id}`\n"
            f"Library range: `{dates['library_start_date']}` s/d `{dates['library_end_date']}`\n"
            f"Hashtag range: `{dates['hashtag_time_range']}`\n"
            f"Selling range: `{dates['selling_time_range']}`"
        )
        
    # ─── Step 1: Fetch Data dari Echotik ─────────────────────
    client = EchotikAPIClient(
        bearer_token=bearer_token,
        regional_headers=REGIONAL_HEADERS,
        timeout=PAGINATION_CONFIG['request_timeout_seconds'],
        max_retries=PAGINATION_CONFIG['max_retries']
    )
    if notifier:
        client.set_notifier(notifier)
        
    raw_video_library = []
    raw_hashtags = []
    raw_video_selling = []
    
    # 1.1 Fetch Video Library
    logging.info("Fetching Video Library...")
    try:
        raw_video_library = client.fetch_video_library(
            start_time=dates['library_start_epoch'],
            end_time=dates['library_end_epoch'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page']
        )
        save_raw_json(raw_video_library, f"video_library_{dates['library_start_date']}_to_{dates['library_end_date']}.json")
    except EchotikAuthException as auth_err:
        logging.critical(f"Auth failed: {auth_err}")
        if notifier:
            notifier.send_message("❌ **Token expired, please refresh** - Pipeline stopped.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error fetching Video Library: {e}")
        if notifier:
            notifier.send_task_failed("fetch_video_library", str(e))
            
    # 1.2 Fetch Hashtags
    logging.info("Fetching Hashtag Leaderboard...")
    try:
        raw_hashtags = client.fetch_hashtags(
            time_range=dates['hashtag_time_range'],
            time_type=HASHTAG_CONFIG['time_type'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page']
        )
        save_raw_json(raw_hashtags, f"hashtags_{dates['hashtag_time_range']}.json")
    except EchotikAuthException as auth_err:
        logging.critical(f"Auth failed: {auth_err}")
        if notifier:
            notifier.send_message("❌ **Token expired, please refresh** - Pipeline stopped.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error fetching Hashtags: {e}")
        if notifier:
            notifier.send_task_failed("fetch_hashtags", str(e))
            
    # 1.3 Fetch Video Selling
    logging.info("Fetching Video Selling...")
    try:
        raw_video_selling = client.fetch_video_selling(
            time_range=dates['selling_time_range'],
            time_type=VIDEO_SELLING_CONFIG['time_type'],
            per_page=PAGINATION_CONFIG['per_page'],
            start_page=PAGINATION_CONFIG['start_page'],
            end_page=PAGINATION_CONFIG['end_page']
        )
        save_raw_json(raw_video_selling, f"video_selling_{dates['selling_time_range']}.json")
    except EchotikAuthException as auth_err:
        logging.critical(f"Auth failed: {auth_err}")
        if notifier:
            notifier.send_message("❌ **Token expired, please refresh** - Pipeline stopped.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error fetching Video Selling: {e}")
        if notifier:
            notifier.send_task_failed("fetch_video_selling", str(e))
            
    # ─── Step 2: Parse & Validate & Category Tagging ────────
    logging.info("Parsing, validating, and tagging categories...")
    
    # 2.1 Parse Video Library
    parsed_video_library = []
    for raw in raw_video_library:
        try:
            parsed = parse_video_library_record(raw, 'All')
            parsed = validate_video_record(parsed)
            parsed_video_library.append(parsed)
        except Exception as e:
            logging.warning(f"Failed to parse video library record: {e}")
            
    # 2.2 Parse Hashtags
    parsed_hashtags = []
    for raw in raw_hashtags:
        try:
            parsed = parse_hashtag_record(raw)
            parsed = validate_hashtag_record(parsed)
            parsed_hashtags.append(parsed)
        except Exception as e:
            logging.warning(f"Failed to parse hashtag record: {e}")
            
    # 2.3 Parse Video Selling
    parsed_video_selling = []
    for raw in raw_video_selling:
        try:
            parsed = parse_video_selling_record(raw, 'All')
            parsed = validate_video_record(parsed)
            parsed_video_selling.append(parsed)
        except Exception as e:
            logging.warning(f"Failed to parse video selling record: {e}")
            
    # Deduplicate
    parsed_video_library = deduplicate_records(parsed_video_library, 'echotik_video_id')
    parsed_hashtags = deduplicate_records(parsed_hashtags, 'echotik_tag_id')
    parsed_video_selling = deduplicate_records(parsed_video_selling, 'echotik_video_id')
    
    # 2.4 Apply Category Tagging (In-Memory)
    hashtag_to_category_counts = {}
    
    # Tag Video Library
    for rec in parsed_video_library:
        rec['category_name'] = classify_text(
            rec.get('title_full'),
            rec.get('title_brief'),
            rec.get('extracted_hashtags')
        )
        
        # Track hashtags mapping
        raw_tags = rec.get('extracted_hashtags')
        tags_list = []
        if raw_tags:
            try:
                if isinstance(raw_tags, str):
                    if raw_tags.startswith('['):
                        tags_list = ast.literal_eval(raw_tags)
                    else:
                        tags_list = [t.strip() for t in raw_tags.split(',') if t.strip()]
                elif isinstance(raw_tags, list):
                    tags_list = raw_tags
            except Exception as e:
                logging.warning(f"Error parsing hashtags: {raw_tags}, error: {e}")
                
        for tag in tags_list:
            tag_clean = str(tag).strip().lower()
            if tag_clean:
                if tag_clean not in hashtag_to_category_counts:
                    hashtag_to_category_counts[tag_clean] = {}
                cat = rec['category_name']
                hashtag_to_category_counts[tag_clean][cat] = hashtag_to_category_counts[tag_clean].get(cat, 0) + 1

    # Tag Video Selling
    for rec in parsed_video_selling:
        rec['category_name'] = classify_text(
            rec.get('title_full'),
            rec.get('title_brief')
        )
        
    # Tag Hashtags Sheet based on Video counts
    hashtag_to_category = {}
    for tag, counts in hashtag_to_category_counts.items():
        hashtag_to_category[tag] = max(counts, key=counts.get)
        
    for rec in parsed_hashtags:
        tag_title = str(rec.get('tag_title', '')).strip().lower()
        if tag_title.startswith('#'):
            tag_title = tag_title[1:]
        rec['category_name'] = hashtag_to_category.get(tag_title, "Lifestyle & Home")
        
    # 2.5 Validation Summary
    all_records = parsed_video_library + parsed_hashtags + parsed_video_selling
    validation_summary = get_validation_summary(all_records)
    logging.info(f"Validation Summary: {validation_summary}")
    
    # 2.6 Export to Excel (fully tagged)
    logging.info("Exporting to Excel...")
    try:
        excel_path_str = export_to_excel(
            video_library_records=parsed_video_library,
            hashtag_records=parsed_hashtags,
            video_selling_records=parsed_video_selling,
            validation_summary=validation_summary,
            output_dir=str(EXCEL_OUTPUT_PATH),
            run_date=file_suffix
        )
        logging.info(f"Excel Export Complete: {excel_path_str}")
        if notifier:
            notifier.send_parser_summary(
                total_records=validation_summary['total'],
                valid_records=validation_summary['valid'],
                invalid_records=validation_summary['invalid'],
                error_breakdown=validation_summary['error_breakdown'],
                excel_filename=excel_filename
            )
    except Exception as e:
        logging.error(f"Failed to export excel: {e}")
        if notifier:
            notifier.send_task_failed("export_to_excel", str(e))
        sys.exit(1)
        
    # ─── Step 3: DB Ingestion (Aiven MySQL) ──────────────────
    logging.info("Connecting to Aiven MySQL for Ingestion...")
    db = EchotikDBHook(connection_string=db_conn_string)
    
    # Test DB Connection
    if not db.test_connection():
        logging.error("Failed to connect to database. Aborting Ingestion.")
        if notifier:
            notifier.send_task_failed("db_connection", "Gagal koneksi ke database Aiven MySQL.")
        sys.exit(1)
    logging.info("Database Connection OK.")
    
    try:
        # 3.1 Standardize columns & Add metadata
        # Convert parsed records back to DataFrames to use standardizing functions
        from utils.ingestion.excel_reader import (
            standardize_video_library_columns,
            standardize_video_selling_columns,
            standardize_hashtag_columns,
            add_ingest_metadata,
            df_to_records
        )
        
        df_lib = pd.DataFrame(parsed_video_library)
        df_tags = pd.DataFrame(parsed_hashtags)
        df_sell = pd.DataFrame(parsed_video_selling)
        
        df_lib = standardize_video_library_columns(df_lib)
        df_lib = add_ingest_metadata(df_lib, run_id, str(excel_path))
        
        df_sell = standardize_video_selling_columns(df_sell)
        df_sell = add_ingest_metadata(df_sell, run_id, str(excel_path))
        
        df_tags = standardize_hashtag_columns(df_tags)
        df_tags = add_ingest_metadata(df_tags, run_id, str(excel_path))
        
        # Combine video_library & video_selling records into all_videos for staging
        lib_records = df_to_records(df_lib)
        sell_records = df_to_records(df_sell)
        
        all_videos = lib_records + sell_records
        all_hashtags = df_to_records(df_tags)
        all_snapshots = build_snapshot_records_from_videos(all_videos)
        
        # 3.2 Load Staging
        logging.info("Loading Staging Tables...")
        videos_inserted = load_videos_to_staging(db, all_videos, batch_size=BATCH_SIZE, truncate_first=True)
        hashtags_inserted = load_hashtags_to_staging(db, all_hashtags, batch_size=BATCH_SIZE, truncate_first=True)
        snapshots_inserted = load_snapshots_to_staging(db, all_snapshots, batch_size=BATCH_SIZE, truncate_first=True)
        
        if notifier:
            notifier.send_message(
                f"**Staging Loaded**\n"
                f"Videos: {videos_inserted}\n"
                f"Hashtags: {hashtags_inserted}\n"
                f"Snapshots: {snapshots_inserted}"
            )
            
        # 3.3 Validate DB Staging
        logging.info("Validating Staging Data...")
        video_staging_summary = validate_video_staging(db, run_id)
        hashtag_staging_summary = validate_hashtag_staging(db, run_id)
        
        total_valid = video_staging_summary['valid'] + hashtag_staging_summary['valid']
        total_invalid = video_staging_summary['invalid'] + hashtag_staging_summary['invalid']
        
        if notifier:
            error_lines = ""
            if video_staging_summary.get('error_breakdown'):
                error_lines = "\n".join([f"  • {k}: {v}" for k, v in video_staging_summary['error_breakdown'].items()])
            notifier.send_message(
                f"**DB Validation Complete**\n"
                f"Videos: {video_staging_summary['valid']} valid, {video_staging_summary['invalid']} invalid\n"
                f"Hashtags: {hashtag_staging_summary['valid']} valid, {hashtag_staging_summary['invalid']} invalid"
                + (f"\n**Errors:**\n{error_lines}" if error_lines else "")
            )
            
        # 3.4 Production Upsert
        logging.info("Performing Production UPSERTs...")
        regions_result = upsert_regions(db, run_id)
        influencers_result = upsert_influencers(db, run_id)
        videos_result = upsert_videos(db, run_id)
        categories_result = upsert_video_categories(db, run_id)
        hashtags_result = upsert_hashtags(db, run_id)
        snapshots_result = insert_video_metrics_snapshots(db, run_id)
        products_result = upsert_products_and_links(db, run_id, sell_records)
        
        if notifier:
            notifier.send_message(
                f"**Production UPSERT Complete**\n"
                f"Regions: {regions_result['inserted']} new\n"
                f"Influencers: {influencers_result['inserted']} ins, {influencers_result['updated']} upd\n"
                f"Videos: {videos_result['inserted']} ins, {videos_result['updated']} upd\n"
                f"Categories: {categories_result['inserted']} tagged\n"
                f"Hashtags: {hashtags_result['inserted']} ins, {hashtags_result['updated']} upd\n"
                f"Snapshots: {snapshots_result['inserted']} appended (time-series)\n"
                f"Products: {products_result['products']} upserted\n"
                f"Video-Products: {products_result['video_products']} linked"
            )
            
        # 3.5 Refresh BI Summary
        if AUTO_REFRESH_BI:
            logging.info("Refreshing BI Summary tables...")
            bi_video_count = refresh_bi_video_summary(db)
            bi_hashtag_count = refresh_bi_hashtag_summary(db)
            if notifier:
                notifier.send_message(
                    f"**BI Summary Refreshed**\n"
                    f"bi_video_revenue_summary: {bi_video_count} rows\n"
                    f"bi_hashtag_revenue_summary: {bi_hashtag_count} rows"
                )
                
        # 3.6 Audit Logging
        logging.info("Writing Audit Log...")
        db.execute(f"""
            INSERT INTO {PRODUCTION_TABLES.get('audit_log', 'ingest_audit_log')} (
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
            'source_file': excel_filename,
            'start_time': dates['run_id'],  # Simple start time
            'end_time': datetime.utcnow().isoformat(),
            'duration': int((datetime.utcnow() - datetime.strptime(run_id, '%Y%m%d_%H%M%S')).total_seconds()),
            'total': len(all_videos) + len(all_hashtags),
            'valid': total_valid,
            'invalid': total_invalid,
            'v_ins': videos_result['inserted'],
            'v_upd': videos_result['updated'],
            'h_ins': hashtags_result['inserted'],
            'h_upd': hashtags_result['updated'],
            's_ins': snapshots_result['inserted'],
            'i_ins': influencers_result['inserted'],
            'i_upd': influencers_result['updated'],
            'status': 'SUCCESS'
        })
        
        # 3.7 Cleanup Staging
        if STAGING_CLEANUP == 'truncate_immediately':
            logging.info("Cleaning up staging tables...")
            for staging_table in STAGING_TABLES.values():
                db.truncate_table(staging_table)
                
        logging.info("Database Ingestion Complete.")
        
        # ─── Final Status ────────────────────────────────────
        duration_total = (datetime.utcnow() - datetime.strptime(run_id, '%Y%m%d_%H%M%S')).total_seconds()
        if notifier:
            notifier.send_dag_summary(
                dag_id='echotik_serverless_pipeline',
                run_id=run_id,
                status='success',
                duration_sec=duration_total,
                total_records=validation_summary['total'],
                next_run="Scheduled via GitHub Actions"
            )
            
    except Exception as db_err:
        logging.critical(f"Database Ingestion failed with critical error: {db_err}", exc_info=True)
        if notifier:
            notifier.send_task_failed("database_ingestion", str(db_err))
        sys.exit(1)
    finally:
        db.close()

    logging.info("Pipeline Complete!")


if __name__ == '__main__':
    main()
