"""
Excel Reader — load Excel file ke pandas DataFrame.

Handle:
- Multiple sheets (video_library, hashtags, video_selling)
- Skip validation_report sheet
- Column normalization (lowercase, strip)
- Type conversion (datetime, numeric)
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional


def scan_excel_files(directory: str, pattern: str = "echotik_data_*.xlsx",
                     exclude_subfolders: List[str] = None) -> List[str]:
    """
    Scan Excel files di directory.
    
    Args:
        directory: Folder yang di-scan
        pattern: Pattern filename (glob)
        exclude_subfolders: List of subfolder names yang di-skip (e.g. ['processed', 'failed'])
    
    Returns:
        List of file paths
    """
    exclude_subfolders = exclude_subfolders or ['processed', 'failed']
    
    dir_path = Path(directory)
    if not dir_path.exists():
        logging.warning(f"Directory not found: {directory}")
        return []
    
    # Glob hanya di root directory, BUKAN recursive
    files = []
    for file in dir_path.glob(pattern):
        # Skip kalau di subfolder yang di-exclude
        relative_path = file.relative_to(dir_path)
        if any(part in exclude_subfolders for part in relative_path.parts):
            continue
        if file.is_file():
            files.append(str(file))
    
    files.sort()  # Sort alphabetical untuk konsistensi
    logging.info(f"Found {len(files)} Excel files in {directory}")
    for f in files:
        logging.info(f"  - {Path(f).name}")
    
    return files


def read_excel_sheets(filepath: str, sheets_to_read: List[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Baca multiple sheets dari Excel file.
    
    Args:
        filepath: Path ke .xlsx
        sheets_to_read: List of sheet names. Default: ['video_library', 'hashtags', 'video_selling']
    
    Returns:
        Dict {sheet_name: DataFrame}
    """
    sheets_to_read = sheets_to_read or ['video_library', 'hashtags', 'video_selling']
    
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Excel file not found: {filepath}")
    
    result = {}
    
    try:
        excel = pd.ExcelFile(filepath, engine='openpyxl')
        available_sheets = excel.sheet_names
        logging.info(f"Available sheets in {Path(filepath).name}: {available_sheets}")
        
        for sheet in sheets_to_read:
            if sheet not in available_sheets:
                logging.warning(f"Sheet '{sheet}' not found, skipping")
                result[sheet] = pd.DataFrame()
                continue
            
            dtypes = {
                'influencer_id': str,
                'echotik_video_id': str,
                'echotik_tag_id': str,
            }
            df = excel.parse(sheet, dtype=dtypes)
            
            # Normalize column names (lowercase, strip whitespace)
            df.columns = [str(c).strip() for c in df.columns]
            
            logging.info(f"Sheet '{sheet}': loaded {len(df)} rows, {len(df.columns)} columns")
            result[sheet] = df
        
        excel.close()
    
    except Exception as e:
        logging.error(f"Failed to read Excel {filepath}: {e}")
        raise
    
    return result


def add_ingest_metadata(df: pd.DataFrame, run_id: str, source_file: str) -> pd.DataFrame:
    """
    Tambah kolom metadata ingest ke DataFrame.
    """
    if df.empty:
        return df
    
    df = df.copy()
    df['ingest_run_id'] = run_id
    df['ingest_excel_file'] = Path(source_file).name
    
    return df


def standardize_video_library_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize kolom video_library untuk match staging table.
    Tambah data_source='library'.
    """
    if df.empty:
        return df
    
    df = df.copy()
    df['data_source'] = 'library'
    
    # Handle missing columns (fill dengan default)
    expected_columns = [
        'echotik_video_id', 'title_full', 'title_brief', 'cover_url', 'video_url',
        'influencer_id', 'influencer_name', 'influencer_unique_id',
        'follower_count_raw', 'follower_count_num', 'influencer_region', 'sales_flag',
        'category_name',
        'duration_raw', 'duration_seconds', 'duration_bucket',
        'views_num', 'likes_num', 'comments_num', 'shares_num',
        'engagement_rate_raw', 'engagement_rate_num',
        'likes_per_views_raw', 'likes_per_views_num',
        'sales_count', 'gmv_usd',
        'is_ai_video', 'is_promote', 'is_latest', 'is_delete',
        'title_length', 'hashtag_count', 'mention_count', 'emoji_count', 'has_question',
        'published_at',
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    # Convert bool columns to TINYINT
    for bool_col in ['is_ai_video', 'is_promote', 'is_latest', 'is_delete', 'has_question']:
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(False).astype(bool).astype(int)
    
    # Convert datetime
    if 'published_at' in df.columns:
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    
    return df


def standardize_video_selling_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize kolom video_selling untuk match staging table.
    Tambah data_source='shop'.
    """
    if df.empty:
        return df
    
    df = df.copy()
    df['data_source'] = 'shop'
    
    expected_columns = [
        'echotik_video_id', 'title_full', 'title_brief', 'cover_url', 'video_url',
        'influencer_id', 'influencer_name', 'influencer_unique_id',
        'follower_count_raw', 'follower_count_num', 'influencer_region',
        'category_name',
        'duration_raw', 'duration_seconds',
        'views_num', 'likes_num', 'comments_num', 'shares_num',
        'interact_ratio_raw', 'interact_ratio_num',
        'total_sale_cnt_num', 'total_gmv_amt_usd',
        'total_gmv_amt_local', 'total_gmv_amt_currency',
        'total_views_count_num',
        'published_at',
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    if 'published_at' in df.columns:
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
    
    return df


def standardize_hashtag_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize kolom hashtags untuk match staging table."""
    if df.empty:
        return df
    
    df = df.copy()
    
    expected_columns = [
        'echotik_tag_id', 'tag_title', 'tag_title_brief',
        'region_id', 'region_name',
        'video_count_raw', 'video_count_num',
        'views_count_raw', 'views_count_num',
        'likes_count_num', 'comments_count_num',
        'shares_count_num', 'favorites_count_num',
        'avg_views_per_video', 'competition_level',
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    return df


def df_to_records(df: pd.DataFrame, columns: List[str] = None) -> List[Dict]:
    """
    Convert DataFrame ke list of dict (untuk bulk insert).
    
    Args:
        df: DataFrame
        columns: List kolom yang di-include. None = semua kolom.
    """
    if df.empty:
        return []
    
    if columns:
        # Filter kolom yang ada di DataFrame
        available = [c for c in columns if c in df.columns]
        df_filtered = df[available]
    else:
        df_filtered = df
    
    # Replace NaN dengan None (untuk SQL)
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    
    # Convert datetime ke string ISO
    for col in df_filtered.columns:
        if df_filtered[col].dtype == 'datetime64[ns]':
            df_filtered[col] = df_filtered[col].astype(str).replace('NaT', None)
    
    records = df_filtered.to_dict(orient='records')
    
    # Final clean — replace 'NaT', 'nan' string ke None
    for rec in records:
        for key, val in rec.items():
            if val in ('NaT', 'nan', 'None'):
                rec[key] = None
    
    return records
