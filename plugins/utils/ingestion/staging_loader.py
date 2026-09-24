"""
Staging Loader — bulk INSERT records ke staging tables.

Flow:
1. Truncate staging table
2. Standardize records
3. Bulk insert (batch 100)
4. Verify count
"""
import logging
from typing import List, Dict


def load_videos_to_staging(db_hook, video_records: List[Dict],
                           staging_table: str = "videos_echotik_staging",
                           batch_size: int = 100,
                           truncate_first: bool = True) -> int:
    """
    Bulk insert videos ke staging table.
    
    Args:
        db_hook: EchotikDBHook instance
        video_records: List of dicts
        staging_table: Nama staging table
        batch_size: Batch insert size
        truncate_first: Truncate sebelum insert?
    
    Returns:
        Jumlah rows inserted
    """
    if not video_records:
        logging.info("No video records to load")
        return 0
    
    if truncate_first:
        db_hook.truncate_table(staging_table)
    
    # Whitelist column yang ada di staging table
    valid_columns = {
        'echotik_video_id', 'data_source',
        'title_full', 'title_brief', 'cover_url', 'video_url',
        'influencer_id', 'influencer_name', 'influencer_unique_id', 'influencer_avatar_url',
        'follower_count_raw', 'follower_count_num', 'influencer_region', 'sales_flag',
        'category_name',
        'duration_raw', 'duration_seconds', 'duration_bucket',
        'views_num', 'likes_num', 'comments_num', 'shares_num',
        'engagement_rate_raw', 'engagement_rate_num',
        'likes_per_views_raw', 'likes_per_views_num',
        'sales_count', 'gmv_usd',
        'total_sale_cnt_num', 'total_gmv_amt_usd',
        'total_gmv_amt_local', 'total_gmv_amt_currency',
        'total_views_count_num', 'interact_ratio_num',
        'is_ai_video', 'is_promote', 'is_latest', 'is_delete',
        'title_length', 'hashtag_count', 'mention_count', 'emoji_count', 'has_question',
        'published_at',
        'ingest_run_id', 'ingest_excel_file',
    }
    
    # Filter records — only include valid columns
    filtered_records = []
    for rec in video_records:
        filtered = {k: v for k, v in rec.items() if k in valid_columns}
        filtered_records.append(filtered)
    
    inserted = db_hook.bulk_insert(staging_table, filtered_records, batch_size=batch_size)
    logging.info(f"Loaded {inserted} videos to {staging_table}")
    return inserted


def load_hashtags_to_staging(db_hook, hashtag_records: List[Dict],
                             staging_table: str = "hashtags_echotik_staging",
                             batch_size: int = 100,
                             truncate_first: bool = True) -> int:
    """Bulk insert hashtags ke staging."""
    if not hashtag_records:
        logging.info("No hashtag records to load")
        return 0
    
    if truncate_first:
        db_hook.truncate_table(staging_table)
    
    valid_columns = {
        'echotik_tag_id', 'tag_title', 'tag_title_brief',
        'region_id', 'region_name',
        'video_count_raw', 'video_count_num',
        'views_count_raw', 'views_count_num',
        'likes_count_num', 'comments_count_num',
        'shares_count_num', 'favorites_count_num',
        'avg_views_per_video', 'competition_level',
        'ingest_run_id', 'ingest_excel_file',
    }
    
    filtered_records = []
    for rec in hashtag_records:
        filtered = {k: v for k, v in rec.items() if k in valid_columns}
        filtered_records.append(filtered)
    
    inserted = db_hook.bulk_insert(staging_table, filtered_records, batch_size=batch_size)
    logging.info(f"Loaded {inserted} hashtags to {staging_table}")
    return inserted


def load_snapshots_to_staging(db_hook, snapshot_records: List[Dict],
                              staging_table: str = "video_metrics_snapshot_staging",
                              batch_size: int = 100,
                              truncate_first: bool = True) -> int:
    """Bulk insert metric snapshots ke staging."""
    if not snapshot_records:
        logging.info("No snapshot records to load")
        return 0
    
    if truncate_first:
        db_hook.truncate_table(staging_table)
    
    valid_columns = {
        'echotik_video_id', 'data_source',
        'views_raw', 'views_num',
        'likes_num', 'comments_num', 'shares_num',
        'engagement_rate_raw', 'engagement_rate_num',
        'likes_per_views_raw', 'likes_per_views_num',
        'sales_count', 'gmv_usd',
        'gmv_local', 'gmv_local_currency',
        'total_sale_cnt_num', 'total_gmv_amt_usd',
        'total_views_count_num', 'interact_ratio_num',
        'ingest_run_id',
    }
    
    filtered_records = []
    for rec in snapshot_records:
        filtered = {k: v for k, v in rec.items() if k in valid_columns}
        filtered_records.append(filtered)
    
    inserted = db_hook.bulk_insert(staging_table, filtered_records, batch_size=batch_size)
    logging.info(f"Loaded {inserted} snapshots to {staging_table}")
    return inserted


def build_snapshot_records_from_videos(video_records: List[Dict]) -> List[Dict]:
    """
    Build snapshot records dari video records.
    Setiap video punya 1 snapshot baru (time-series).
    """
    snapshots = []
    
    for v in video_records:
        snap = {
            'echotik_video_id': v.get('echotik_video_id'),
            'data_source': v.get('data_source', 'library'),
            'views_num': v.get('views_num', 0) or 0,
            'likes_num': v.get('likes_num', 0) or 0,
            'comments_num': v.get('comments_num', 0) or 0,
            'shares_num': v.get('shares_num', 0) or 0,
            'engagement_rate_raw': v.get('engagement_rate_raw'),
            'engagement_rate_num': v.get('engagement_rate_num'),
            'likes_per_views_raw': v.get('likes_per_views_raw'),
            'likes_per_views_num': v.get('likes_per_views_num'),
            'sales_count': v.get('sales_count', 0) or 0,
            'gmv_usd': v.get('gmv_usd', 0) or 0,
            'gmv_local': v.get('total_gmv_amt_local', 0) or 0,
            'gmv_local_currency': v.get('total_gmv_amt_currency'),
            'total_sale_cnt_num': v.get('total_sale_cnt_num', 0) or 0,
            'total_gmv_amt_usd': v.get('total_gmv_amt_usd', 0) or 0,
            'total_views_count_num': v.get('total_views_count_num', 0) or 0,
            'interact_ratio_num': v.get('interact_ratio_num'),
            'ingest_run_id': v.get('ingest_run_id'),
        }
        snapshots.append(snap)
    
    return snapshots
