"""
deployment.py
Prefect deployment configuration for production-grade scheduling.

Usage:
    # Create deployment (recommended - interactive)
    python deployment.py create
    
    # Deploy and run immediately
    python deployment.py deploy
    
    # Run locally without deployment
    python deployment.py run-local
    
    # Or use CLI directly:
    prefect deploy prefect_flow.py:run_pipeline_flow --name morocco-re-dev
"""

import argparse
import os
from datetime import timedelta

from prefect import flow
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule

from prefect_flow import run_pipeline_flow


# ---------------------------------------------------------------------------
# DEPLOYMENTS
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefect Deployment Manager")
    parser.add_argument(
        "command",
        choices=["create", "deploy", "run-local"],
        help="Command to execute",
    )
    parser.add_argument(
        "--env",
        "-e",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment to target",
    )
    args = parser.parse_args()

    if args.command == "run-local":
        print("Running pipeline locally...")
        run_pipeline_flow(source="csv", max_pages=1, table_name="listings_test", environment="dev")
        print("✓ Pipeline completed")

    elif args.command == "create":
        print(f"Creating deployment for {args.env}...")
        print("\nUse the Prefect CLI:")
        print("  prefect deploy prefect_flow.py:run_pipeline_flow")
        print("\nOr with parameters:")
        print("  prefect deploy prefect_flow.py:run_pipeline_flow \\")
        print("    --name morocco-re-dev \\")
        print("    --param source=csv")
        
        env_config = {
            "dev": {
                "name": "morocco-re-dev",
                "source": "csv",
                "schedule": "0 8 * * *",
            },
            "staging": {
                "name": "morocco-re-staging",
                "source": "scraper",
                "schedule": "0 6 * * *",
            },
            "prod": {
                "name": "morocco-re-prod",
                "source": "scraper",
                "schedule": "0 6 * * *",
            },
        }
        
        config = env_config[args.env]
        print(f"\nFor {args.env}, run:")
        print(f"  prefect deploy prefect_flow.py:run_pipeline_flow \\")
        print(f"    --name {config['name']} \\")
        print(f"    --cron '{config['schedule']}'")

    elif args.command == "deploy":
        print("Deploying with Prefect 3.x...")
        print("\nRun this command:")
        print("  prefect deploy prefect_flow.py:run_pipeline_flow")
