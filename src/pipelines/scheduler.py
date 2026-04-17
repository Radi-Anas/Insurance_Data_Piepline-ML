"""
pipeline/scheduler.py
Prefect pipeline for automated ETL and model training.

Schedule:
    - ETL runs daily at 2 AM
    - Model retraining runs weekly on Sundays at 3 AM

Usage:
    # Local testing
    python pipeline/scheduler.py

    # Deploy to Prefect Cloud
    python pipeline/scheduler.py deploy

    # Run manually
    python pipeline/scheduler.py run
"""

import logging
from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.deployments import Deployment

from src.data.ingestion.claims_etl import run_etl
from src.models.fraud_model import main as train_model

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(name="extract_transform_load", retries=2, retry_delay_seconds=60)
def run_etl_task():
    """Run ETL pipeline task."""
    log = get_run_logger()
    log.info("Starting ETL pipeline...")
    
    try:
        metrics = run_etl()
        log.info(f"ETL completed: {metrics}")
        return metrics
    except Exception as e:
        log.error(f"ETL failed: {e}")
        raise


@task(name="train_model", retries=2, retry_delay_seconds=120)
def train_model_task():
    """Run model training task."""
    log = get_run_logger()
    log.info("Starting model training...")
    
    try:
        train_model()
        log.info("Model training completed")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        log.error(f"Model training failed: {e}")
        raise


@task(name="validate_model", retries=1)
def validate_model_task():
    """Validate model performance."""
    log = get_run_logger()
    log.info("Validating model...")
    
    from src.models.fraud_model import load_data, prepare_features, train_model
    import pandas as pd
    
    try:
        df = load_data()
        X, y, encoders, features = prepare_features(df)
        results = train_model(X, y)
        
        min_accuracy = 0.70
        min_auc = 0.70
        
        if results['accuracy'] < min_accuracy:
            raise ValueError(f"Model accuracy {results['accuracy']} below threshold {min_accuracy}")
        
        if results['auc_score'] < min_auc:
            raise ValueError(f"Model AUC {results['auc_score']} below threshold {min_auc}")
        
        log.info(f"Model validation passed: accuracy={results['accuracy']}, auc={results['auc_score']}")
        return results
    
    except Exception as e:
        log.error(f"Model validation failed: {e}")
        raise


@task(name="health_check")
def health_check_task():
    """Run health check after pipeline completion."""
    log = get_run_logger()
    log.info("Running health check...")
    
    from configs.settings import DATABASE_URL
    from sqlalchemy import create_engine, text
    
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT COUNT(*) FROM claims"))
        engine.dispose()
        log.info("Health check passed")
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        log.error(f"Health check failed: {e}")
        raise


@flow(name="insurance-fraud-pipeline", log_prints=True)
def full_pipeline():
    """
    Full pipeline: ETL → Train Model → Validate → Health Check
    
    This is the main orchestration flow that runs on schedule.
    """
    log = get_run_logger()
    log.info("=== Starting Insurance Fraud Pipeline ===")
    
    # Step 1: Run ETL
    etl_result = run_etl_task()
    log.info(f"ETL Result: {etl_result}")
    
    # Step 2: Train model
    model_result = train_model_task()
    log.info(f"Model Training Result: {model_result}")
    
    # Step 3: Validate model
    validation_result = validate_model_task()
    log.info(f"Validation Result: {validation_result}")
    
    # Step 4: Health check
    health_result = health_check_task()
    log.info(f"Health Check: {health_result}")
    
    log.info("=== Pipeline Complete ===")
    
    return {
        "pipeline_status": "success",
        "etl": etl_result,
        "model": validation_result,
        "health": health_result,
        "timestamp": datetime.now().isoformat(),
    }


@flow(name="etl-pipeline-only", log_prints=True)
def etl_pipeline():
    """
    ETL only pipeline - runs daily.
    """
    log = get_run_logger()
    log.info("=== Starting ETL Pipeline ===")
    
    result = run_etl_task()
    
    log.info("=== ETL Pipeline Complete ===")
    return result


# Schedule: Daily at 2 AM for ETL
etl_schedule = Schedule(
    clocks=[CronClock("0 2 * * *")]  # Daily at 2 AM
)

# Schedule: Weekly on Sunday at 3 AM for full pipeline
full_pipeline_schedule = Schedule(
    clocks=[CronClock("0 3 * * 0)")]  # Weekly on Sundays at 3 AM
)


def deploy():
    """Deploy pipelines to Prefect."""
    from prefect.deployments import Deployment
    from prefect.infrastructure import Docker
    
    # Deploy ETL pipeline
    etl_deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="etl-daily",
        schedule=etl_schedule,
        infrastructure=Docker(),
    )
    etl_deployment.apply()
    
    # Deploy full pipeline (weekly)
    full_deployment = Deployment.build_from_flow(
        flow=full_pipeline,
        name="full-pipeline-weekly",
        schedule=full_pipeline_schedule,
        infrastructure=Docker(),
    )
    full_deployment.apply()
    
    print("Deployed to Prefect Cloud")


def run():
    """Run pipeline locally."""
    full_pipeline()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "deploy":
            deploy()
        elif command == "run":
            run()
    else:
        print("Usage: python pipeline/scheduler.py [deploy|run]")
