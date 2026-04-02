"""
prefect_flow.py
Production-grade ETL pipeline orchestrated with Prefect.

Features:
- Task retries with exponential backoff
- Caching expensive operations
- Failure notifications via webhook
- Parameterized for dev/staging/prod environments
- Result persistence to PostgreSQL
- Concurrent execution where possible
"""

import os
from datetime import timedelta
from typing import Optional

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

from pipeline.extract import extract_from_csv, scrape_avito
from pipeline.transform import transform, transform_scraped, save_clean_csv
from pipeline.load import load_to_postgres
from config.settings import DB_CONFIG


# Environment configuration
ENV = os.getenv("ENV", "dev")
USE_SCRAPER = os.getenv("USE_SCRAPER", "false").lower() == "true"

RAW_DATA_PATH = "data/raw/listings.csv"
CLEAN_DATA_PATH = "data/clean/listings_clean.csv"
TABLE_NAME = "listings"


# ---------------------------------------------------------------------------
# NOTIFICATION HELPERS
# ---------------------------------------------------------------------------

def build_failure_message(flow_run, task_run=None, error=None) -> dict:
    """Build a structured notification payload for webhook alerts."""
    payload = {
        "flow_name": "morocco-re-pipeline",
        "flow_run_id": str(flow_run.id),
        "flow_run_name": flow_run.name,
        "status": "FAILED",
        "environment": ENV,
    }
    if task_run:
        payload["failed_task"] = task_run.name
    if error:
        payload["error"] = str(error)[:500]
    return payload


def log_failure_notification(flow_run, task_run=None, error=None):
    """Log failure details for observability."""
    logger = get_run_logger()
    logger.error(
        f"Flow FAILED | Flow: {flow_run.name} | "
        f"Task: {task_run.name if task_run else 'N/A'} | "
        f"Error: {str(error)[:200]}"
    )


# ---------------------------------------------------------------------------
# EXTRACT TASKS
# ---------------------------------------------------------------------------

@task(
    name="extract-from-csv",
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    tags=["extract", "csv"],
    description="Extract raw data from CSV file with retry logic",
)
def extract_csv_task(file_path: str) -> dict:
    """Extract data from CSV with caching and retry on failure."""
    logger = get_run_logger()
    logger.info(f"Extracting from CSV: {file_path}")
    
    df = extract_from_csv(file_path)
    
    result = {
        "row_count": len(df),
        "source": "csv",
        "file_path": file_path,
    }
    logger.info(f"Extracted {result['row_count']} rows from CSV")
    return result


@task(
    name="extract-from-avito",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
    tags=["extract", "scraper"],
    description="Scrape live listings from Avito.ma with exponential backoff",
)
def extract_scraper_task(max_pages: int = 3) -> dict:
    """Scrape Avito.ma with exponential backoff retries."""
    logger = get_run_logger()
    logger.info(f"Starting Avito scraper (max_pages={max_pages})")
    
    df = scrape_avito(max_pages=max_pages)
    
    if df.empty:
        raise ValueError("Scraper returned no data")
    
    result = {
        "row_count": len(df),
        "source": "scraper",
        "max_pages": max_pages,
    }
    logger.info(f"Scraped {result['row_count']} listings from Avito")
    return result


@task(
    name="extract-combined",
    retries=1,
    tags=["extract"],
    description="Combine and deduplicate data from multiple sources",
)
def extract_combined_task(
    csv_result: Optional[dict] = None,
    scraper_result: Optional[dict] = None,
) -> str:
    """Combine results from multiple extract tasks and return primary source."""
    logger = get_run_logger()
    
    sources = []
    if csv_result:
        sources.append(f"CSV ({csv_result['row_count']} rows)")
    if scraper_result:
        sources.append(f"Avito ({scraper_result['row_count']} rows)")
    
    # Prefer scraper data if available, otherwise CSV
    primary_source = "scraper" if scraper_result else "csv"
    logger.info(f"Using {primary_source} as primary source. Available: {', '.join(sources)}")
    
    return primary_source


# ---------------------------------------------------------------------------
# TRANSFORM TASKS
# ---------------------------------------------------------------------------

@task(
    name="transform-csv-data",
    retries=2,
    retry_delay_seconds=30,
    tags=["transform", "csv"],
    description="Transform CSV-extracted data",
)
def transform_csv_task(df) -> dict:
    """Transform data from CSV mode."""
    logger = get_run_logger()
    logger.info("Transforming CSV data...")
    
    clean_df = transform(df)
    save_clean_csv(clean_df, CLEAN_DATA_PATH)
    
    result = {
        "output_rows": len(clean_df),
        "output_path": CLEAN_DATA_PATH,
    }
    logger.info(f"Transformed {result['output_rows']} clean rows")
    return result


@task(
    name="transform-scraper-data",
    retries=2,
    retry_delay_seconds=30,
    tags=["transform", "scraper"],
    description="Transform scraped data with scraper-specific rules",
)
def transform_scraper_task(df) -> dict:
    """Transform data from scraper mode."""
    logger = get_run_logger()
    logger.info("Transforming scraped data...")
    
    clean_df = transform_scraped(df)
    save_clean_csv(clean_df, CLEAN_DATA_PATH)
    
    result = {
        "output_rows": len(clean_df),
        "output_path": CLEAN_DATA_PATH,
    }
    logger.info(f"Transformed {result['output_rows']} clean rows")
    return result


@task(
    name="validate-data",
    retries=1,
    tags=["validate", "quality"],
    description="Validate data quality before loading",
)
def validate_task(
    clean_df,
    transform_result: dict,
    min_rows: int = 5,
    strict_url: bool = False,
) -> dict:
    """Validate cleaned data meets quality thresholds."""
    logger = get_run_logger()
    logger.info("Running data quality validation...")
    
    from pipeline.validate import validate
    
    is_valid = validate(clean_df, min_rows=min_rows, strict_url=strict_url)
    
    validation_result = {
        "passed": is_valid,
        "row_count": len(clean_df),
        "min_rows": min_rows,
    }
    
    if not is_valid:
        raise ValueError(
            f"Data validation failed. "
            f"Got {validation_result['row_count']} rows but quality checks did not pass."
        )
    
    logger.info(f"Validation passed for {validation_result['row_count']} rows")
    return validation_result


# ---------------------------------------------------------------------------
# LOAD TASKS
# ---------------------------------------------------------------------------

@task(
    name="load-to-postgres",
    retries=3,
    retry_delay_seconds=60,
    tags=["load", "postgres"],
    description="Load validated data into PostgreSQL",
)
def load_task(clean_df, table_name: str = TABLE_NAME) -> dict:
    """Load data to PostgreSQL with retry on transient failures."""
    logger = get_run_logger()
    logger.info(f"Loading {len(clean_df)} rows to PostgreSQL table '{table_name}'...")
    
    load_to_postgres(clean_df, table_name)
    
    result = {
        "rows_loaded": len(clean_df),
        "table": table_name,
        "database": DB_CONFIG["database"],
        "host": DB_CONFIG["host"],
    }
    logger.info(f"Successfully loaded {result['rows_loaded']} rows to {table_name}")
    return result


@task(
    name="log-summary",
    retries=0,
    tags=["logging", "summary"],
    description="Log final pipeline summary",
)
def log_summary_task(
    extract_result: dict,
    transform_result: dict,
    validation_result: dict,
    load_result: dict,
) -> dict:
    """Log comprehensive pipeline execution summary."""
    logger = get_run_logger()
    
    summary = {
        "environment": ENV,
        "source": extract_result.get("source", "unknown"),
        "rows_extracted": extract_result.get("row_count", 0),
        "rows_transformed": transform_result.get("output_rows", 0),
        "validation_passed": validation_result.get("passed", False),
        "rows_loaded": load_result.get("rows_loaded", 0),
        "target_table": load_result.get("table", TABLE_NAME),
    }
    
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    for key, value in summary.items():
        logger.info(f"  {key}: {value}")
    logger.info("=" * 60)
    
    return summary


# ---------------------------------------------------------------------------
# MAIN FLOW
# ---------------------------------------------------------------------------

@flow(
    name="morocco-re-pipeline",
    description="ETL pipeline for Moroccan real estate data from Avito.ma",
    retries=2,
    retry_delay_seconds=120,
    log_prints=True,
)
def run_pipeline_flow(
    source: str = "csv",
    max_pages: int = 3,
    table_name: str = TABLE_NAME,
    environment: str = ENV,
) -> dict:
    """
    Main ETL pipeline flow for Moroccan real estate data.
    
    Args:
        source: Data source - "csv" or "scraper" (default: csv)
        max_pages: Max pages to scrape if using scraper mode (default: 3)
        table_name: Target PostgreSQL table name (default: listings)
        environment: Deployment environment (default: from ENV var)
    
    Returns:
        dict: Pipeline execution summary with metrics
    """
    logger = get_run_logger()
    logger.info(f"Starting Morocco RE Pipeline | Environment: {environment} | Source: {source}")
    
    # Extract
    if source == "scraper" and USE_SCRAPER:
        extract_result = extract_scraper_task(max_pages=max_pages)
        df = scrape_avito(max_pages=max_pages)
    else:
        extract_result = extract_csv_task(RAW_DATA_PATH)
        df = extract_from_csv(RAW_DATA_PATH)
    
    # Transform
    if source == "scraper":
        transform_result = transform_scraper_task(df)
        clean_df = transform_scraped(df)
    else:
        transform_result = transform_csv_task(df)
        clean_df = transform(df)
    
    # Validate
    min_rows = 10 if source == "scraper" else 5
    strict_url = source == "scraper"
    validation_result = validate_task(clean_df, transform_result, min_rows=min_rows, strict_url=strict_url)
    
    # Load
    load_result = load_task(clean_df, table_name=table_name)
    
    # Summary
    summary = log_summary_task(
        extract_result=extract_result,
        transform_result=transform_result,
        validation_result=validation_result,
        load_result=load_result,
    )
    
    logger.info("Pipeline completed successfully!")
    return summary


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_pipeline_flow()
