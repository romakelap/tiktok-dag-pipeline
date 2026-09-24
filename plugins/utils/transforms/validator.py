"""
Validator — quality check untuk parsed records.

Handle:
- Schema validation (required fields)
- Business rule validation (>= 0, valid date)
- Deduplication (same ID detected)
- Mark records as valid/invalid
"""
import logging
from typing import List, Dict, Set


def validate_video_record(record: Dict) -> Dict:
    """Validate 1 video record dan tambah is_valid flag"""
    errors = []
    
    # Required fields
    if not record.get('echotik_video_id'):
        errors.append('missing_video_id')
    if not record.get('title_full'):
        errors.append('missing_title')
    if not record.get('published_at'):
        errors.append('missing_published_at')
    
    # Business rules
    if record.get('views_num', 0) < 0:
        errors.append('negative_views')
    if record.get('likes_num', 0) < 0:
        errors.append('negative_likes')
    if record.get('gmv_usd', 0) < 0:
        errors.append('negative_gmv')
    if record.get('duration_seconds', 0) < 0:
        errors.append('negative_duration')
    
    record['is_valid'] = len(errors) == 0
    record['validation_errors'] = errors
    return record


def validate_hashtag_record(record: Dict) -> Dict:
    """Validate 1 hashtag record"""
    errors = []
    
    if not record.get('echotik_tag_id'):
        errors.append('missing_tag_id')
    if not record.get('tag_title'):
        errors.append('missing_tag_title')
    
    if record.get('video_count_num', 0) < 0:
        errors.append('negative_video_count')
    if record.get('views_count_num', 0) < 0:
        errors.append('negative_views_count')
    
    record['is_valid'] = len(errors) == 0
    record['validation_errors'] = errors
    return record


def deduplicate_records(records: List[Dict], key: str = 'echotik_video_id') -> List[Dict]:
    """
    Mark duplicates dalam list of records.
    Keep first occurrence sebagai valid, sisanya marked duplicate.
    """
    seen: Set[str] = set()
    
    for record in records:
        record_id = record.get(key, '')
        
        if not record_id:
            continue  # Skip kalau ID kosong (sudah ditangkap validator)
        
        if record_id in seen:
            # Duplicate detected
            if 'validation_errors' not in record:
                record['validation_errors'] = []
            record['validation_errors'].append('duplicate')
            record['is_valid'] = False
        else:
            seen.add(record_id)
    
    return records


def get_validation_summary(records: List[Dict]) -> Dict:
    """
    Generate summary dari hasil validasi.
    Return:
        {
            'total': 1000,
            'valid': 950,
            'invalid': 50,
            'error_breakdown': {
                'duplicate': 20,
                'negative_views': 15,
                'missing_title': 15
            }
        }
    """
    total = len(records)
    valid = sum(1 for r in records if r.get('is_valid', False))
    invalid = total - valid
    
    error_breakdown: Dict[str, int] = {}
    for record in records:
        if not record.get('is_valid', False):
            for error in record.get('validation_errors', []):
                error_breakdown[error] = error_breakdown.get(error, 0) + 1
    
    return {
        'total': total,
        'valid': valid,
        'invalid': invalid,
        'error_breakdown': error_breakdown,
    }
