import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# Setup path to import from parent directory (db.py)
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from db import get_engine

def export_dataset():
    print("[START] Exporting category dataset...")
    engine = get_engine()
    
    # Ensure artifacts directory exists
    artifact_dir = ROOT_DIR / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = artifact_dir / "category_training_dataset.csv"
    
    # Fetch title_full, hashtag_text, nlp_text from vw_ml_feature_store
    # Joined with video_category_tags to get core_category
    query = """
        SELECT 
            fs.video_pk,
            fs.title_full,
            fs.hashtag_text,
            fs.nlp_text,
            t.core_category
        FROM vw_ml_feature_store fs
        JOIN video_category_tags t ON fs.video_pk = t.video_pk
        WHERE t.tagging_method = 'rule_based'
    """
    
    print("[DB] Loading dataset from MySQL...")
    try:
        df = pd.read_sql(query, engine)
        print(f"[DB] Loaded {len(df)} records.")
    except Exception as e:
        print(f"[ERROR] Failed to query database: {e}")
        sys.exit(1)
        
    if df.empty:
        print("[WARNING] No records found with tagging_method = 'rule_based'.")
        print("[WARNING] Please check if rule-based classification has been seeded.")
        sys.exit(1)
        
    print("[PROCESS] Combining text fields...")
    # Fill missing values and combine fields
    df["title_full"] = df["title_full"].fillna("")
    df["hashtag_text"] = df["hashtag_text"].fillna("")
    df["nlp_text"] = df["nlp_text"].fillna("")
    
    # Create combined_text column
    df["combined_text"] = df.apply(
        lambda r: f"{r['title_full']} {r['hashtag_text']} {r['nlp_text']}".strip(),
        axis=1
    )
    
    # Select only required columns for training
    df_export = df[["video_pk", "combined_text", "core_category"]]
    
    print(f"[EXPORT] Saving {len(df_export)} records to {output_path}...")
    df_export.to_csv(output_path, index=False, encoding="utf-8")
    print("[SUCCESS] Dataset exported successfully.")

if __name__ == "__main__":
    export_dataset()
