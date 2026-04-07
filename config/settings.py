"""
config/settings.py
Environment-aware configuration loader.
Supports: development, staging, production
"""

import os
from dotenv import load_dotenv

# Load environment from ENV var, default to development
ENV = os.getenv("ENV", "development").lower()

# Load env-specific .env file
env_file = f".env.{ENV}" if ENV != "development" else ".env"
load_dotenv(env_file)

# Database configuration per environment
ENV_CONFIGS = {
    "development": {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "insurance_fraud"),
        "pool_size": 5,
        "max_overflow": 10,
    },
    "staging": {
        "host": os.getenv("DB_HOST", "staging-db.example.com"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "insurance_fraud_staging"),
        "pool_size": 10,
        "max_overflow": 20,
    },
    "production": {
        "host": os.getenv("DB_HOST", "prod-db.example.com"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "insurance_fraud_prod"),
        "pool_size": 20,
        "max_overflow": 40,
    },
}

# Get config for current environment
db_config = ENV_CONFIGS.get(ENV, ENV_CONFIGS["development"])

DB_CONFIG = {
    "host": db_config["host"],
    "port": db_config["port"],
    "database": db_config["database"],
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "pool_size": db_config["pool_size"],
    "max_overflow": db_config["max_overflow"],
}

# SQLAlchemy connection string
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Logging configuration per environment
LOG_CONFIG = {
    "development": {"level": "DEBUG", "format": "simple"},
    "staging": {"level": "INFO", "format": "json"},
    "production": {"level": "WARNING", "format": "json"},
}

log_config = LOG_CONFIG.get(ENV, LOG_CONFIG["development"])
LOG_LEVEL = log_config["level"]
LOG_FORMAT = log_config["format"]

# Application settings
APP_CONFIG = {
    "debug": ENV == "development",
    "environment": ENV,
    "api_title": "Insurance Claims Fraud API",
    "api_version": "1.0.0",
}
