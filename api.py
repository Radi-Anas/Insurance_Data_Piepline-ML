"""
api.py
FastAPI endpoint for Insurance Claims Fraud Detection.

Run:
    uvicorn api:app --reload

Endpoints:
    GET  /                    - API info
    GET  /health              - Health check
    POST /predict             - Predict fraud for a claim
    POST /predict/batch       - Batch predict for multiple claims
    GET  /model/metrics       - Model performance metrics
    GET  /predictions         - Prediction history
    GET  /stats               - Fraud statistics
    GET  /claims              - List claims

Authentication:
    Set API_KEY in .env for protected endpoints.
    Use header: X-API-Key: your_key
"""

from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from functools import lru_cache
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import json
import hashlib

from config.settings import DATABASE_URL, APP_CONFIG

# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'
)
logger = logging.getLogger(__name__)


def log_request(endpoint: str, params: dict = None):
    """Log API request in structured format."""
    logger.info(json.dumps({"endpoint": endpoint, "params": params}))


def log_error(endpoint: str, error: str):
    """Log API error in structured format."""
    logger.error(json.dumps({"endpoint": endpoint, "error": error}))


# API Key Authentication
import os
API_KEY = os.getenv("API_KEY", "")


def verify_api_key(x_api_key: str = Header(None)):
    """Verify API key for protected endpoints."""
    if not API_KEY:
        return None
    
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="Missing API key")
    
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    
    return x_api_key


# Simple in-memory cache
class Cache:
    """Simple TTL cache for API responses."""
    
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl = timedelta(seconds=ttl_seconds)
    
    def get(self, key: str):
        """Get value from cache if not expired."""
        if key in self.cache:
            value, expiry = self.cache[key]
            if datetime.now() < expiry:
                return value
            del self.cache[key]
        return None
    
    def set(self, key: str, value):
        """Set value in cache with TTL."""
        expiry = datetime.now() + self.ttl
        self.cache[key] = (value, expiry)
    
    def clear(self):
        """Clear all cache."""
        self.cache = {}


cache = Cache(ttl_seconds=300)

app = FastAPI(
    title="Insurance Claims Fraud API",
    description="ML-powered fraud detection for insurance claims",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory prediction history (for demo - use database in production)
prediction_history = []


def get_db_connection():
    """Create database connection."""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database unavailable")


def get_model():
    """Load fraud detection model."""
    try:
        from fraud_model import load_model
        return load_model()
    except Exception as e:
        logger.error(f"Model load failed: {e}")
        return None


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "name": "Insurance Claims Fraud API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /health",
            "POST /predict",
            "POST /predict/batch",
            "GET /model/metrics",
            "GET /predictions",
            "GET /stats",
            "GET /claims",
        ],
    }


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Check model
        model = get_model()
        model_status = "loaded" if model else "not loaded"
        
        return {"status": "healthy", "database": "connected", "model": model_status}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/predict")
def predict_fraud(claim_data: dict) -> dict:
    """
    Predict fraud for an insurance claim.
    
    Example payload:
    {
        "months_as_customer": 12,
        "age": 35,
        "policy_state": "OH",
        "policy_csl": "250/500",
        "policy_deductable": 500,
        "policy_annual_premium": 1200,
        "insured_sex": "M",
        "insured_education_level": "BS",
        "insured_occupation": "Tech",
        "incident_type": "Single Vehicle Collision",
        "incident_severity": "Major Damage",
        "total_claim_amount": 5000,
        "vehicle_claim": 3000,
        "property_claim": 1500,
        "injury_claim": 500,
        "auto_make": "Toyota"
    }
    """
    try:
        model_data = get_model()
        if model_data is None:
            raise HTTPException(status_code=503, detail="Model not available")
        
        from fraud_model import predict_fraud
        result = predict_fraud(claim_data, model_data)
        
        prediction_result = {
            "prediction": result["is_fraud"],
            "fraud_probability": result["fraud_probability"],
            "confidence": result["confidence"],
            "risk_level": "HIGH" if result["fraud_probability"] > 0.7 else "MEDIUM" if result["fraud_probability"] > 0.3 else "LOW"
        }
        
        # Store in history
        prediction_history.append({
            "timestamp": datetime.now().isoformat(),
            "claim_data": claim_data,
            **prediction_result
        })
        
        return prediction_result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch")
def predict_batch(claims: List[dict]) -> dict:
    """
    Predict fraud for multiple claims at once.
    
    Example payload:
    [
        {"months_as_customer": 12, "age": 35, ...},
        {"months_as_customer": 24, "age": 45, ...}
    ]
    """
    try:
        model_data = get_model()
        if model_data is None:
            raise HTTPException(status_code=503, detail="Model not available")
        
        from fraud_model import predict_fraud
        
        results = []
        for claim_data in claims:
            result = predict_fraud(claim_data, model_data)
            results.append({
                "prediction": result["is_fraud"],
                "fraud_probability": result["fraud_probability"],
                "confidence": result["confidence"],
                "risk_level": "HIGH" if result["fraud_probability"] > 0.7 else "MEDIUM" if result["fraud_probability"] > 0.3 else "LOW"
            })
        
        # Summary
        fraud_count = sum(1 for r in results if r["prediction"] == 1)
        
        return {
            "total_predictions": len(results),
            "fraud_count": fraud_count,
            "legitimate_count": len(results) - fraud_count,
            "fraud_rate": round(fraud_count / len(results) * 100, 1),
            "results": results
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/model/metrics")
def get_model_metrics(x_api_key: str = Header(None)) -> dict:
    """Get model performance metrics (requires API key if configured)."""
    verify_api_key(x_api_key)
    
    try:
        model_data = get_model()
        if model_data is None:
            raise HTTPException(status_code=503, detail="Model not available")
        
        # Load data and retrain to get fresh metrics
        from fraud_model import load_data, prepare_features, train_model
        
        df = load_data()
        X, y, encoders, feature_names = prepare_features(df)
        results = train_model(X, y)
        
        return {
            "accuracy": round(results["accuracy"], 3),
            "auc_score": round(results["auc_score"], 3),
            "model_type": "RandomForest",
            "features_count": len(feature_names),
            "total_samples": len(y),
            "fraud_samples": int(sum(y)),
            "legitimate_samples": int(len(y) - sum(y)),
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictions")
def get_predictions(
    limit: int = Query(50, ge=1, le=100),
    fraud_only: bool = Query(False, description="Filter to fraud predictions only"),
    x_api_key: str = Header(None),
) -> dict:
    """Get prediction history (requires API key if configured)."""
    verify_api_key(x_api_key)
    global prediction_history
    
    filtered = prediction_history
    if fraud_only:
        filtered = [p for p in prediction_history if p["prediction"] == 1]
    
    return {
        "count": len(filtered),
        "limit": limit,
        "predictions": filtered[:limit]
    }


@app.get("/stats")
def get_stats() -> dict:
    """Get fraud statistics (cached for 5 minutes)."""
    # Check cache
    cache_key = "stats"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    
    try:
        engine = get_db_connection()
        
        df = pd.read_sql_query(text("""
            SELECT 
                COUNT(*) as total_claims,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
                ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as fraud_rate,
                ROUND(AVG(total_claim_amount::numeric), 2) as avg_claim_amount,
                ROUND(AVG(policy_annual_premium::numeric), 2) as avg_premium
            FROM claims
        """), engine)
        
        engine.dispose()
        
        result = {
            "total_claims": int(df.iloc[0]["total_claims"]),
            "fraud_count": int(df.iloc[0]["fraud_count"]),
            "fraud_rate_percent": float(df.iloc[0]["fraud_rate"]),
            "avg_claim_amount": float(df.iloc[0]["avg_claim_amount"]),
            "avg_premium": float(df.iloc[0]["avg_premium"]),
        }
        
        # Cache result
        cache.set(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/claims")
def get_claims(
    fraud_only: bool = Query(False, description="Filter to fraud claims only"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Get claims with optional filters."""
    try:
        engine = get_db_connection()
        query = "SELECT * FROM claims"
        
        if fraud_only:
            query += " WHERE is_fraud = 1"
        
        query += f" LIMIT {limit} OFFSET {offset}"
        
        df = pd.read_sql_query(text(query), engine)
        engine.dispose()
        
        return {
            "count": len(df),
            "limit": limit,
            "offset": offset,
            "data": df.to_dict(orient="records"),
        }
    
    except Exception as e:
        logger.error(f"Error fetching claims: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
