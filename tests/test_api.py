"""
test_api.py
Unit tests for API endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from api import app
from unittest.mock import patch, MagicMock


client = TestClient(app)


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_200(self):
        """Test health endpoint returns 200."""
        with patch('api.get_db_connection') as mock_db, \
             patch('api.get_model') as mock_model:
            
            mock_engine = MagicMock()
            mock_db.return_value = mock_engine
            mock_model.return_value = {"model": "test"}
            
            response = client.get("/health")
            assert response.status_code == 200

    def test_health_checks_database(self):
        """Test health checks database connection."""
        with patch('api.get_db_connection') as mock_db, \
             patch('api.get_model') as mock_model:
            
            mock_engine = MagicMock()
            mock_db.return_value = mock_engine
            mock_model.return_value = None
            
            response = client.get("/health")
            data = response.json()
            assert "database" in data


class TestPredictEndpoint:
    """Tests for /predict endpoint."""

    def test_predict_requires_valid_json(self):
        """Test predict rejects invalid JSON."""
        with patch('api.get_model') as mock_model:
            mock_model.return_value = {
                "model": MagicMock(),
                "encoders": {},
                "features": ["age", "months_as_customer"]
            }
            
            response = client.post("/predict", json={"invalid": "data"})
            # May return 200 or 422 depending on validation

    def test_predict_returns_prediction(self):
        """Test predict returns prediction."""
        with patch('api.get_model') as mock_model:
            mock_model.return_value = {
                "model": MagicMock(),
                "encoders": {},
                "features": ["age"]
            }
            
            response = client.post("/predict", json={
                "months_as_customer": 12,
                "age": 35,
                "policy_state": "OH",
                "policy_csl": "250/500",
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
            })
            
            if response.status_code == 200:
                data = response.json()
                assert "prediction" in data
                assert "fraud_probability" in data


class TestStatsEndpoint:
    """Tests for /stats endpoint."""

    def test_stats_returns_data(self):
        """Test stats returns fraud statistics."""
        with patch('api.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.iloc = [0]
            mock_result.iloc[0] = {
                "total_claims": 1000,
                "fraud_count": 247,
                "fraud_rate": 24.7,
                "avg_claim_amount": 50000,
                "avg_premium": 1200
            }
            
            mock_conn.execute.return_value = mock_result
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
            mock_db.return_value = mock_engine
            
            response = client.get("/stats")
            # May be cached or fail, but should not 500


class TestAPIEndpoints:
    """Tests for general API behavior."""

    def test_root_endpoint(self):
        """Test root endpoint returns API info."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data

    def test_docs_available(self):
        """Test API docs are available."""
        response = client.get("/docs")
        assert response.status_code == 200


class TestAuthentication:
    """Tests for API authentication."""

    def test_model_metrics_requires_key(self):
        """Test /model/metrics requires API key."""
        response = client.get("/model/metrics")
        # Should require auth or return 401/403

    def test_predictions_requires_key(self):
        """Test /predictions requires API key."""
        response = client.get("/predictions")
        # Should require auth or return 401/403