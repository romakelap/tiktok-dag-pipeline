import os
import sys
import pandas as pd
from pathlib import Path

ml_service_path = Path("/Users/nicorevaldo/Desktop/STIKOM/TA/ml-service")
sys.path.append(str(ml_service_path))

from db import get_engine

def main():
    engine = get_engine()
    output_dir = Path("/Users/nicorevaldo/Desktop/STIKOM/TA/raw-data")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "ml_feature_store.parquet"
    
    print("Querying vw_ml_feature_store...")
    query = "SELECT * FROM vw_ml_feature_store"
    
    # Load into Pandas
    df = pd.read_sql(query, con=engine)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns.")
    
    # Save as Parquet
    print(f"Saving to {output_file}...")
    df.to_parquet(output_file, index=False)
    print("Export complete!")

if __name__ == "__main__":
    main()
