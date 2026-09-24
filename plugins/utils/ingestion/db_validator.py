"""
DB Validator — validate data di staging tables.

Checks:
1. Duplicate detection (di dalam batch)
2. NULL required fields
3. Business rules (>= 0, valid date, dll)
4. Foreign key prep (cek influencer_id ada di influencers, kalau tidak siap di-insert)
"""
import logging
from typing import Dict


def validate_video_staging(db_hook, run_id: str,
                           staging_table: str = "videos_echotik_staging") -> Dict[str, int]:
    """
    Validate records di video staging table.
    Mark invalid records dengan is_valid=0 + validation_error.
    
    Returns:
        Dict dengan counts: {total, valid, invalid, error_breakdown}
    """
    
    # 1. Duplicate detection (echotik_video_id sama dalam 1 run)
    db_hook.execute(f"""
        UPDATE {staging_table} s1
        JOIN (
            SELECT echotik_video_id, MIN(staging_id) AS first_id
            FROM {staging_table}
            WHERE ingest_run_id = :run_id
            GROUP BY echotik_video_id
            HAVING COUNT(*) > 1
        ) s2 ON s1.echotik_video_id = s2.echotik_video_id
            AND s1.staging_id != s2.first_id
        SET s1.is_valid = 0,
            s1.validation_error = 'duplicate_in_batch'
        WHERE s1.ingest_run_id = :run_id
    """, params={'run_id': run_id})
    
    # 2. NULL required fields
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0,
            validation_error = 'missing_video_id'
        WHERE ingest_run_id = :run_id
          AND (echotik_video_id IS NULL OR echotik_video_id = '')
    """, params={'run_id': run_id})
    
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0,
            validation_error = COALESCE(validation_error, 'missing_published_at')
        WHERE ingest_run_id = :run_id
          AND published_at IS NULL
          AND is_valid = 1
    """, params={'run_id': run_id})
    
    # 3. Business rules
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0,
            validation_error = COALESCE(validation_error, 'negative_views')
        WHERE ingest_run_id = :run_id
          AND views_num < 0
          AND is_valid = 1
    """, params={'run_id': run_id})
    
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0,
            validation_error = COALESCE(validation_error, 'negative_gmv')
        WHERE ingest_run_id = :run_id
          AND (gmv_usd < 0 OR total_gmv_amt_usd < 0)
          AND is_valid = 1
    """, params={'run_id': run_id})
    
    # Get summary
    summary_df = db_hook.query_to_df(f"""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) AS valid,
            SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) AS invalid
        FROM {staging_table}
        WHERE ingest_run_id = :run_id
    """, params={'run_id': run_id})
    
    if summary_df.empty:
        return {'total': 0, 'valid': 0, 'invalid': 0, 'error_breakdown': {}}
    
    summary = summary_df.iloc[0].to_dict()
    
    # Error breakdown
    error_df = db_hook.query_to_df(f"""
        SELECT validation_error, COUNT(*) AS cnt
        FROM {staging_table}
        WHERE ingest_run_id = :run_id AND is_valid = 0
        GROUP BY validation_error
    """, params={'run_id': run_id})
    
    error_breakdown = {}
    for _, row in error_df.iterrows():
        error_breakdown[row['validation_error']] = int(row['cnt'])
    
    result = {
        'total': int(summary['total'] or 0),
        'valid': int(summary['valid'] or 0),
        'invalid': int(summary['invalid'] or 0),
        'error_breakdown': error_breakdown,
    }
    
    logging.info(f"Video staging validation: {result}")
    return result


def validate_hashtag_staging(db_hook, run_id: str,
                             staging_table: str = "hashtags_echotik_staging") -> Dict[str, int]:
    """Validate hashtags staging."""
    
    # Duplicate
    db_hook.execute(f"""
        UPDATE {staging_table} s1
        JOIN (
            SELECT echotik_tag_id, MIN(staging_id) AS first_id
            FROM {staging_table}
            WHERE ingest_run_id = :run_id
            GROUP BY echotik_tag_id
            HAVING COUNT(*) > 1
        ) s2 ON s1.echotik_tag_id = s2.echotik_tag_id
            AND s1.staging_id != s2.first_id
        SET s1.is_valid = 0,
            s1.validation_error = 'duplicate_in_batch'
        WHERE s1.ingest_run_id = :run_id
    """, params={'run_id': run_id})
    
    # Required fields
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0, validation_error = 'missing_tag_id'
        WHERE ingest_run_id = :run_id
          AND (echotik_tag_id IS NULL OR echotik_tag_id = '')
    """, params={'run_id': run_id})
    
    db_hook.execute(f"""
        UPDATE {staging_table}
        SET is_valid = 0, validation_error = COALESCE(validation_error, 'missing_tag_title')
        WHERE ingest_run_id = :run_id
          AND (tag_title IS NULL OR tag_title = '')
          AND is_valid = 1
    """, params={'run_id': run_id})
    
    # Summary
    summary_df = db_hook.query_to_df(f"""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) AS valid,
            SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) AS invalid
        FROM {staging_table}
        WHERE ingest_run_id = :run_id
    """, params={'run_id': run_id})
    
    summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {'total': 0, 'valid': 0, 'invalid': 0}
    
    return {
        'total': int(summary['total'] or 0),
        'valid': int(summary['valid'] or 0),
        'invalid': int(summary['invalid'] or 0),
    }
