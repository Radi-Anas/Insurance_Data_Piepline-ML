"""
Synthetic Data Generator for Insurance Claims

Generates realistic synthetic insurance claims data for training
and testing ML models. Uses smart augmentation to preserve fraud patterns.

Usage:
    python -c "from src.data.ingestion.synthetic_data import generate_data; generate_data(10000)"
"""

import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POLICY_STATES = ['OH', 'IN', 'IL', 'PA', 'NY']
POLICY_CSL = ['100/300', '250/500', '500/1000']
SEVERITIES = ['Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss']
INCIDENT_TYPES = [
    'Single Vehicle Collision', 'Multi-Vehicle Collision', 
    'Vehicle Rollover', 'Hit and Run', 'Parked Car Damage',
    'Broken Windshield', 'Fire Damage', 'Theft'
]
VEHICLE_MAKES = ['Honda', 'Toyota', 'Ford', 'Chevrolet', 'BMW', 'Mercedes', 'Audi', 'Nissan', 'Mazda']
OCCUPATIONS = ['Tech', 'Medical', 'Education', 'Finance', 'Retail', 'Construction', 'Legal', 'Other']
EDUCATION_LEVELS = ['High School', 'BS', 'MS', 'PhD']

np.random.seed(42)
random.seed(42)


def generate_policy_number():
    """Generate unique policy number."""
    return f"POL{''.join([str(random.randint(0, 9)) for _ in range(8)]}"


def generate_customer_data(n_samples: int, fraud_rate: float = 0.24) -> pd.DataFrame:
    """Generate base customer data with realistic distributions."""
    
    data = {
        'policy_number': [generate_policy_number() for _ in range(n_samples)],
        'age': np.random.randint(18, 75, n_samples),
        'months_as_customer': np.random.randint(0, 240, n_samples),
        'policy_state': np.random.choice(POLICY_STATES, n_samples),
        'policy_csl': np.random.choice(POLICY_CSL, n_samples),
        'policy_deductable': np.random.choice([500, 1000, 1500, 2000], n_samples),
        'policy_annual_premium': np.round(np.random.uniform(500, 5000, n_samples), 2),
        'insured_sex': np.random.choice(['M', 'F'], n_samples),
        'insured_education_level': np.random.choice(EDUCATION_LEVELS, n_samples),
        'insured_occupation': np.random.choice(OCCUPATIONS, n_samples),
        'capital-gains': np.random.uniform(0, 50000, n_samples),
        'capital-loss': np.random.uniform(0, 30000, n_samples),
    }
    
    return pd.DataFrame(data)


def generate_incident_data(n_samples: int) -> pd.DataFrame:
    """Generate incident data with realistic correlations."""
    
    # Bias toward certain incident types for fraud patterns
    severities = np.random.choice(SEVERITIES, n_samples, p=[0.3, 0.35, 0.25, 0.1])
    
    # Higher severity = more severe claim amounts
    severity_multiplier = {
        'Trivial Damage': 0.3,
        'Minor Damage': 0.5,
        'Major Damage': 0.8,
        'Total Loss': 1.5
    }
    multipliers = [severity_multiplier[s] for s in severities]
    
    data = {
        'incident_type': np.random.choice(INCIDENT_TYPES, n_samples),
        'incident_severity': severities,
        'incident_hour_of_the_day': np.random.randint(0, 24, n_samples),
        'number_of_vehicles_involved': np.random.choice([1, 2, 3], n_samples, p=[0.6, 0.3, 0.1]),
        'bodily_injuries': np.random.choice([0, 1, 2, 3], n_samples, p=[0.5, 0.3, 0.15, 0.05]),
        'witnesses': np.random.choice([0, 1, 2], n_samples, p=[0.4, 0.4, 0.2]),
        'property_damage': np.random.choice(['YES', 'NO'], n_samples, p=[0.7, 0.3]),
        'police_report_available': np.random.choice(['YES', 'NO'], n_samples, p=[0.6, 0.4]),
    }
    
    base_amounts = np.random.uniform(1000, 15000, n_samples)
    data['total_claim_amount'] = np.round(base_amounts * np.array(multipliers), 2)
    
    return pd.DataFrame(data)


def generate_vehicle_data(n_samples: int) -> pd.DataFrame:
    """Generate vehicle data."""
    current_year = datetime.now().year
    
    data = {
        'auto_make': np.random.choice(VEHICLE_MAKES, n_samples),
        'auto_year': np.random.randint(current_year - 20, current_year, n_samples),
    }
    
    return pd.DataFrame(data)


def inject_fraud_patterns(df: pd.DataFrame, fraud_rate: float = 0.24) -> pd.DataFrame:
    """
    Inject fraud patterns to make detection realistic.
    These patterns mimic real fraud indicators.
    """
    n_fraud = int(len(df) * fraud_rate)
    fraud_indices = np.random.choice(len(df), n_fraud, replace=False)
    
    # Make certain features correlate with fraud
    for idx in fraud_indices:
        # High claim relative to premium
        df.loc[idx, 'total_claim_amount'] = df.loc[idx, 'policy_annual_premium'] * np.random.uniform(3, 8)
        
        # New customer + high claim = suspicious
        if df.loc[idx, 'months_as_customer'] < 12:
            df.loc[idx, 'total_claim_amount'] *= np.random.uniform(1.5, 3)
        
        # No witness + bodily injuries = very suspicious
        if df.loc[idx, 'bodily_injuries'] > 0 and df.loc[idx, 'witnesses'] == 0:
            df.loc[idx, 'total_claim_amount'] *= np.random.uniform(1.2, 2)
    
    return df


def generate_claims_data(n_samples: int = 10000, fraud_rate: float = 0.24) -> pd.DataFrame:
    """
    Generate complete synthetic insurance claims dataset.
    
    Args:
        n_samples: Number of claims to generate
        fraud_rate: Target fraud rate (0.0 - 1.0)
    
    Returns:
        DataFrame with all claim features + is_fraud target
    """
    logger.info(f"Generating {n_samples} synthetic claims with {fraud_rate*100:.1f}% fraud rate...")
    
    # Generate base data
    customer_df = generate_customer_data(n_samples, fraud_rate)
    incident_df = generate_incident_data(n_samples)
    vehicle_df = generate_vehicle_data(n_samples)
    
    # Combine
    df = pd.concat([customer_df, incident_df, vehicle_df], axis=1)
    
    # Create claim breakdown
    df['vehicle_claim'] = np.round(df['total_claim_amount'] * np.random.uniform(0.4, 0.8, n_samples), 2)
    df['property_claim'] = np.round(df['total_claim_amount'] * np.random.uniform(0.1, 0.4, n_samples), 2)
    df['injury_claim'] = np.round(df['total_claim_amount'] * np.random.uniform(0, 0.3, n_samples), 2)
    
    # Add fraud indicator
    df['fraud_reported'] = 'N'
    fraud_indices = np.random.choice(df.index, size=int(n_samples * fraud_rate), replace=False)
    df.loc[fraud_indices, 'fraud_reported'] = 'Y'
    
    # Apply fraud patterns to make it learnable
    df = inject_fraud_patterns(df, fraud_rate)
    
    logger.info(f"Generated {len(df)} claims. Fraud rate: {df['fraud_reported'].value_counts()['Y']/len(df)*100:.1f}%")
    
    return df


def augment_original_data(original_df: pd.DataFrame, target_size: int = 10000) -> pd.DataFrame:
    """
    Augment original dataset by adding smart noise.
    Preserves fraud patterns while increasing variety.
    """
    logger.info(f"Augmenting original {len(original_df)} → {target_size} samples...")
    
    required_cols = ['policy_number', 'age', 'months_as_customer', 'policy_state',
                   'policy_csl', 'policy_annual_premium', 'insured_sex',
                   'fraud_reported']
    
    # Check columns exist
    available_cols = [c for c in required_cols if c in original_df.columns]
    df = original_df[available_cols].copy()
    
    # Duplicate and add noise until we reach target
    while len(df) < target_size:
        noise_df = df.sample(min(len(df), target_size - len(df))).copy()
        
        # Add small random noise to numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            noise = np.random.normal(0, 0.05 * df[col].std(), len(noise_df))
            noise_df[col] = noise_df[col] + noise
        
        df = pd.concat([df, noise_df], ignore_index=True)
    
    logger.info(f"Augmented to {len(df)} samples")
    return df


# Remove policy number duplicates from augmentation
df = df.drop_duplicates(subset=['policy_number'], keep='first')


if __name__ == "__main__":
    df = generate_claims_data(10000, fraud_rate=0.24)
    df.to_csv("data/raw/insurance_claims_synthetic.csv", index=False)
    print(f"Saved {len(df)} synthetic claims")