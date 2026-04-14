"""
claims_etl.py
ETL pipeline for insurance claims data.

Extracts → Transforms → Validates → Loads claims data to PostgreSQL.

Usage:
    python -c "from claims_etl import run_etl; run_etl()"
"""

import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from configs.settings import DATABASE_URL
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
RAW_DATA_PATH = "data/raw/insurance_claims.csv"
CLEAN_DATA_PATH = "data/clean/claims_clean.csv"
TABLE_NAME = "claims"

# Data quality rules
REQUIRED_COLUMNS = [
    'policy_number', 'age', 'months_as_customer', 'policy_state',
    'policy_annual_premium', 'total_claim_amount', 'is_fraud'
]

NUMERIC_VALIDATION = {
    'age': {'min': 18, 'max': 100},
    'months_as_customer': {'min': 0, 'max': 500},
    'policy_annual_premium': {'min': 0, 'max': 100000},
    'total_claim_amount': {'min': 0, 'max': 1000000},
}

CATEGORICAL_VALUES = {
    'policy_state': ['Oh', 'In', 'Il', 'Pa', 'Ny'],
    'insured_sex': ['M', 'F'],
    'incident_severity': ['Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss'],
}


def extract_from_csv(file_path: str) -> pd.DataFrame:
    """Extract raw claims data from CSV."""
    logger.info(f"Extracting from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Extracted {len(df)} rows")
    return df


def get_engine():
    """Create and return a SQLAlchemy engine with connection pooling."""
    from configs.settings import POOL_CONFIG
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_size=POOL_CONFIG["pool_size"],
            max_overflow=POOL_CONFIG["max_overflow"],
            pool_pre_ping=POOL_CONFIG["pool_pre_ping"],
            pool_recycle=POOL_CONFIG["pool_recycle"],
            pool_timeout=POOL_CONFIG["pool_timeout"],
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def validate_data(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Validate data quality and return list of issues.
    
    Returns:
        dict with validation issues
    """
    issues = {}
    
    # Check required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        issues['missing_columns'] = missing_cols
    
    # Validate numeric ranges
    for col, constraints in NUMERIC_VALIDATION.items():
        if col in df.columns:
            violations = df[
                (df[col] < constraints['min']) | 
                (df[col] > constraints['max'])
            ]
            if len(violations) > 0:
                issues[f'{col}_out_of_range'] = f"{len(violations)} rows out of range [{constraints['min']}, {constraints['max']}]"
    
    # Validate categorical values
    for col, valid_values in CATEGORICAL_VALUES.items():
        if col in df.columns:
            invalid = df[~df[col].isin(valid_values + ['Unknown'])]
            if len(invalid) > 0:
                issues[f'{col}_invalid_values'] = f"{len(invalid)} rows with invalid values"
    
    # Check for duplicates
    if 'policy_number' in df.columns:
        dupes = df['policy_number'].duplicated().sum()
        if dupes > 0:
            issues['duplicates'] = f"{dupes} duplicate policy numbers"
    
    return issues


def validate_and_raise(df: pd.DataFrame) -> None:
    """Validate data and raise exception if critical issues found."""
    issues = validate_data(df)
    
    critical_issues = ['missing_columns']
    
    found_critical = [i for i in critical_issues if i in issues]
    if found_critical:
        raise ValueError(f"Critical validation issues: {issues}")
    
    # Log warnings for non-critical
    if issues:
        logger.warning(f"Validation warnings: {issues}")


def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform insurance claims data:
    - Clean column names
    - Handle missing values
    - Encode categorical features
    - Calculate derived features
    """
    logger.info("Transforming claims data...")
    initial_count = len(df)
    
    # ---- Step 1: Remove junk columns ----
    cols_to_drop = [col for col in df.columns if col.startswith('_') or col == '_c39']
    df = df.drop(columns=cols_to_drop, errors='ignore')
    logger.info(f"Removed {len(cols_to_drop)} junk columns")
    
    # ---- Step 2: Handle missing values ----
    # Fill numeric with median
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())
    
    # Fill categorical with 'Unknown'
    cat_cols = df.select_dtypes(include=['object']).columns
    for col in cat_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna('Unknown')
    
    # ---- Step 3: Clean categorical columns ----
    for col in cat_cols:
        if col != 'fraud_reported':
            df[col] = df[col].astype(str).str.strip().str.title()
    
    # ---- Step 4: Create derived features ----
    # Total claims amount
    if 'total_claim_amount' in df.columns:
        df['has_claim'] = (df['total_claim_amount'] > 0).astype(int)
    
    # Age groups
    if 'age' in df.columns:
        df['age_group'] = pd.cut(
            df['age'], 
            bins=[0, 25, 35, 45, 55, 65, 100],
            labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
        )
    
    # Customer tenure groups
    if 'months_as_customer' in df.columns:
        df['tenure_group'] = pd.cut(
            df['months_as_customer'],
            bins=[0, 12, 24, 48, 120, 500],
            labels=['0-1yr', '1-2yr', '2-4yr', '4-10yr', '10yr+']
        )
    
    # ---- Step 5: Encode target variable ----
    if 'fraud_reported' in df.columns:
        df['is_fraud'] = (df['fraud_reported'] == 'Y').astype(int)
    
    # ---- Step 6: Select final columns ----
    final_cols = [
        'months_as_customer', 'age', 'policy_number', 'policy_state',
        'policy_csl', 'policy_deductable', 'policy_annual_premium',
        'insured_sex', 'insured_education_level', 'insured_occupation',
        'capital-gains', 'capital-loss',
        'incident_type', 'collision_type', 'incident_severity',
        'incident_hour_of_the_day', 'number_of_vehicles_involved',
        'property_damage', 'bodily_injuries', 'witnesses',
        'police_report_available', 'total_claim_amount',
        'injury_claim', 'property_claim', 'vehicle_claim',
        'auto_make', 'auto_year',
        'has_claim', 'age_group', 'tenure_group', 'is_fraud'
    ]
    
    # Keep only columns that exist
    final_cols = [c for c in final_cols if c in df.columns]
    df = df[final_cols]
    
    dropped = initial_count - len(df)
    logger.info(f"Transformation complete: {len(df)} rows ({dropped} dropped)")
    
    return df


def load_to_postgres(df: pd.DataFrame, table_name: str = "claims") -> int:
    """Write the clean DataFrame to PostgreSQL."""
    logger.info(f"Loading {len(df)} rows to '{table_name}'...")
    
    # Ensure numeric types
    for col in ['policy_deductable', 'months_as_customer', 'age',
                'policy_annual_premium', 'capital-gains', 'capital-loss',
                'incident_hour_of_the_day', 'number_of_vehicles_involved',
                'bodily_injuries', 'witnesses', 'total_claim_amount',
                'injury_claim', 'property_claim', 'vehicle_claim', 'auto_year', 'is_fraud']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    engine = get_engine()
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
        logger.info(f"Successfully loaded {len(df)} rows")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise
    finally:
        engine.dispose()


def run_etl(mode: str = "full", validate: bool = True) -> dict:
    """
    Run the complete ETL pipeline.
    
    Args:
        mode: "full" (extract + transform + load) or "load_only" (skip extract)
    
    Returns:
        dict with metrics
    """
    import time
    from datetime import datetime
    
    start_time = time.time()
    metrics = {
        "run_timestamp": datetime.now().isoformat(),
        "rows_extracted": 0,
        "rows_loaded": 0,
        "execution_time_seconds": 0,
    }
    
    try:
        # Extract
        if mode == "full":
            raw_df = extract_from_csv(RAW_DATA_PATH)
            metrics["rows_extracted"] = len(raw_df)
            
            # Transform
            clean_df = transform_claims(raw_df)
            
            # Validate
            if validate:
                validate_and_raise(clean_df)
            
            # Save clean CSV
            clean_df.to_csv(CLEAN_DATA_PATH, index=False)
        else:
            clean_df = pd.read_csv(CLEAN_DATA_PATH)
            metrics["rows_extracted"] = len(clean_df)
        
        # Load
        rows_loaded = load_to_postgres(clean_df, TABLE_NAME)
        metrics["rows_loaded"] = rows_loaded
        
        metrics["status"] = "success"
        logger.info(f"ETL complete: {rows_loaded} rows loaded in {time.time() - start_time:.1f}s")
        
    except Exception as e:
        metrics["status"] = "failed"
        metrics["error"] = str(e)
        logger.error(f"ETL failed: {e}")
        raise
    
    metrics["execution_time_seconds"] = round(time.time() - start_time, 2)
    return metrics


if __name__ == "__main__":
    result = run_etl()
    print(f"\nETL Result: {result}")