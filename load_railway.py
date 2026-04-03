"""
load_railway.py
Load data from local CSV to Railway PostgreSQL.

Usage:
    python load_railway.py
"""

import pandas as pd
from sqlalchemy import create_engine
from pipeline.transform import transform

# Railway PostgreSQL URL (public)
DATABASE_URL = "postgresql://postgres:xsMyDADlYlVWmSJpVNscxrGLbVZEgOIS@junction.proxy.rlwy.net:27799/railway"

TABLE = "listings"


def main():
    print("Loading data to Railway PostgreSQL...")
    
    # Load CSV
    df = pd.read_csv("data/raw/listings.csv")
    print(f"Loaded {len(df)} rows from CSV")
    
    # Transform
    clean = transform(df)
    print(f"Transformed to {len(clean)} rows")
    
    # Load to Railway
    engine = create_engine(DATABASE_URL)
    clean.to_sql(
        name=TABLE,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    print(f"Loaded to Railway PostgreSQL table '{TABLE}'")
    print("Done!")


if __name__ == "__main__":
    main()
