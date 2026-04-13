"""
main.py
Pipeline entry point for Insurance Claims Fraud Detection.

Runs: ETL → Train Model → Start API + Dashboard

Usage:
    python main.py

Configuration:
    Set MODE = "full" to run ETL + model training
    Set MODE = "api_only" to skip ETL/model and just start services
"""

import logging
import subprocess
import sys
import os

from src.pipelines.logging_config import setup_logging

setup_logging()

from src.data.ingestion.claims_etl import run_etl
from src.models.fraud_model import main as train_model

logger = logging.getLogger(__name__)

MODE = "full"


def run_pipeline():
    """Execute the complete pipeline."""
    logger.info("=== Starting Insurance Claims Pipeline ===")
    
    metrics = {}
    
    try:
        logger.info("Step 1: Running ETL...")
        metrics = run_etl()
        logger.info(f"ETL complete: {metrics}")
        
        logger.info("Step 2: Training fraud detection model...")
        train_model()
        logger.info("Model training complete")
        
        logger.info("=== Pipeline finished successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


def start_services():
    """Start Streamlit dashboard and API."""
    logger.info("Starting services...")
    
    dashboard_proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "src/services/dashboard.py", "--server.port", "8501"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    api_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    print("\n" + "=" * 50)
    print("Services started!")
    print("Dashboard: http://localhost:8501")
    print("API:      http://localhost:8000")
    print("API Docs: http://localhost:8000/docs")
    print("=" * 50 + "\n")
    
    return dashboard_proc, api_proc


def main():
    """Main entry point."""
    if MODE == "full":
        try:
            run_pipeline()
        except Exception as e:
            logger.warning(f"Pipeline error: {e}. Starting services anyway...")
    
    dashboard_proc, api_proc = start_services()
    
    try:
        dashboard_proc.wait()
    except KeyboardInterrupt:
        logger.info("Shutting down services...")
        dashboard_proc.terminate()
        api_proc.terminate()


if __name__ == "__main__":
    main()
