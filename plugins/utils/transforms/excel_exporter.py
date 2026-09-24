"""
Excel Exporter — save parsed records ke Excel file dengan multiple sheets.
"""
import pandas as pd
import logging
from typing import List, Dict
from pathlib import Path
from datetime import datetime


def export_to_excel(
    video_library_records: List[Dict],
    hashtag_records: List[Dict],
    video_selling_records: List[Dict],
    validation_summary: Dict,
    output_dir: str,
    run_date: str
) -> str:
    """
    Export semua parsed records ke Excel multi-sheet.
    
    Args:
        video_library_records: List of parsed video library records
        hashtag_records: List of parsed hashtag records
        video_selling_records: List of parsed video selling records
        validation_summary: Dict dari get_validation_summary()
        output_dir: Directory untuk save file
        run_date: Format "YYYYMMDD_HHMMSS" untuk filename
    
    Returns:
        Full path ke Excel file yang di-generate
    """
    # Build filename & path
    filename = f"echotik_data_{run_date}.xlsx"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / filename
    
    # Build DataFrames
    df_video_library = pd.DataFrame(video_library_records) if video_library_records else pd.DataFrame()
    df_hashtags = pd.DataFrame(hashtag_records) if hashtag_records else pd.DataFrame()
    df_video_selling = pd.DataFrame(video_selling_records) if video_selling_records else pd.DataFrame()
    
    # Validation summary as DataFrame
    summary_rows = [
        {'metric': 'Total Records', 'value': validation_summary.get('total', 0)},
        {'metric': 'Valid Records', 'value': validation_summary.get('valid', 0)},
        {'metric': 'Invalid Records', 'value': validation_summary.get('invalid', 0)},
    ]
    for error_type, count in validation_summary.get('error_breakdown', {}).items():
        summary_rows.append({'metric': f"Error: {error_type}", 'value': count})
    
    df_summary = pd.DataFrame(summary_rows)
    
    # Write to Excel
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if not df_video_library.empty:
                df_video_library.to_excel(writer, sheet_name='video_library', index=False)
                logging.info(f"Sheet 'video_library': {len(df_video_library)} rows")
            
            if not df_hashtags.empty:
                df_hashtags.to_excel(writer, sheet_name='hashtags', index=False)
                logging.info(f"Sheet 'hashtags': {len(df_hashtags)} rows")
            
            if not df_video_selling.empty:
                df_video_selling.to_excel(writer, sheet_name='video_selling', index=False)
                logging.info(f"Sheet 'video_selling': {len(df_video_selling)} rows")
            
            df_summary.to_excel(writer, sheet_name='validation_report', index=False)
            logging.info(f"Sheet 'validation_report': {len(df_summary)} rows")
        
        logging.info(f"Excel file created: {output_path}")
        return str(output_path)
    
    except Exception as e:
        logging.error(f"Failed to export Excel: {e}")
        raise
