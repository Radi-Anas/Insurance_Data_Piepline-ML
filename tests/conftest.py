"""
conftest.py
Pytest fixtures for insurance fraud detection pipeline tests.
"""

import pandas as pd
import pytest


@pytest.fixture
def sample_claim_df():
    """Sample raw insurance claim DataFrame."""
    return pd.DataFrame({
        "policy_number": ["POL12345678", "POL87654321", "POL11223344"],
        "months_as_customer": [24, 6, 120],
        "age": [35, 28, 55],
        "policy_state": ["OH", "NY", "IL"],
        "policy_csl": ["250/500", "100/300", "500/1000"],
        "policy_annual_premium": [1200.0, 800.0, 2000.0],
        "insured_sex": ["M", "F", "M"],
        "incident_type": [
            "Single Vehicle Collision",
            "Multi-Vehicle Collision",
            "Vehicle Rollover"
        ],
        "incident_severity": ["Minor Damage", "Major Damage", "Total Loss"],
        "total_claim_amount": [5000.0, 15000.0, 25000.0],
        "witnesses": [2, 0, 1],
        "bodily_injuries": [0, 2, 1],
        "auto_make": ["Toyota", "BMW", "Honda"],
        "fraud_reported": ["N", "Y", "N"],
    })


@pytest.fixture
def sample_clean_claim():
    """Sample cleaned claim data for prediction."""
    return {
        "months_as_customer": 24,
        "age": 35,
        "policy_state": "OH",
        "policy_csl": "250/500",
        "policy_annual_premium": 1200,
        "incident_type": "Single Vehicle Collision",
        "incident_severity": "Minor Damage",
        "total_claim_amount": 5000,
        "auto_make": "Toyota",
        "witnesses": 2,
        "bodily_injuries": 0,
    }


@pytest.fixture
def sample_high_risk_claim():
    """Sample high fraud risk claim."""
    return {
        "months_as_customer": 3,
        "age": 25,
        "policy_state": "NY",
        "policy_csl": "100/300",
        "policy_annual_premium": 800,
        "incident_type": "Multi-Vehicle Collision",
        "incident_severity": "Major Damage",
        "total_claim_amount": 15000,
        "auto_make": "BMW",
        "witnesses": 0,
        "bodily_injuries": 2,
    }


@pytest.fixture
def dirty_claim_df():
    """DataFrame with invalid/missing data."""
    return pd.DataFrame({
        "policy_number": [None, "POL123", "POL456"],
        "age": [35, -5, 150],  # Invalid ages
        "total_claim_amount": [5000, None, -1000],  # Invalid amounts
        "witnesses": [0, 0, 10],  # Unrealistic witness count
    })
