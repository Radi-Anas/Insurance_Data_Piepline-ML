"""
fraud_model.py
ML model for insurance claims fraud detection.

Trains XGBoost classifier to predict fraudulent claims.
Saves model and provides prediction function.

Usage:
    python fraud_model.py  # Train and save model
    
    from fraud_model import load_model, predict_fraud
    model = load_model()
    result = predict_fraud(claim_data)
"""

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    roc_auc_score,
    precision_recall_curve
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Model path
MODEL_PATH = "models/fraud_model.pkl"
ENCODER_PATH = "models/label_encoders.pkl"


def load_data() -> pd.DataFrame:
    """Load cleaned claims data."""
    df = pd.read_csv("data/clean/claims_clean.csv")
    logger.info(f"Loaded {len(df)} claims")
    return df


def prepare_features(df: pd.DataFrame):
    """
    Prepare features for model training.
    Handles categorical encoding, feature engineering, and selection.
    """
    # Target
    y = df['is_fraud'].values
    
    # Drop target and non-features
    drop_cols = ['is_fraud', 'policy_number']  # policy_number is unique ID
    feature_cols = [c for c in df.columns if c not in drop_cols]
    X = df[feature_cols].copy()
    
    # Feature Engineering - create interaction features
    if 'total_claim_amount' in X.columns and 'policy_annual_premium' in X.columns:
        # Claim to premium ratio
        X['claim_to_premium_ratio'] = X['total_claim_amount'] / (X['policy_annual_premium'] + 1)
    
    if 'vehicle_claim' in X.columns and 'property_claim' in X.columns:
        # Vehicle vs property damage
        X['vehicle_property_ratio'] = X['vehicle_claim'] / (X['property_claim'] + 1)
    
    if 'injury_claim' in X.columns and 'total_claim_amount' in X.columns:
        # Injury severity
        X['injury_ratio'] = X['injury_claim'] / (X['total_claim_amount'] + 1)
    
    if 'age' in X.columns and 'months_as_customer' in X.columns:
        # Customer tenure relative to age
        X['tenure_age_ratio'] = X['months_as_customer'] / (X['age'] * 12 + 1)
    
    if 'bodily_injuries' in X.columns and 'witnesses' in X.columns:
        # Injuries without witnesses (potential fraud indicator)
        X['no_witness_injury'] = ((X['bodily_injuries'] > 0) & (X['witnesses'] == 0)).astype(int)
    
    if 'number_of_vehicles_involved' in X.columns and 'witnesses' in X.columns:
        # Complex incident without witnesses
        X['complex_no_witness'] = ((X['number_of_vehicles_involved'] > 1) & (X['witnesses'] == 0)).astype(int)
    
    if 'policy_deductable' in X.columns and 'total_claim_amount' in X.columns:
        # Deductible to claim ratio
        X['deductible_claim_ratio'] = X['policy_deductable'] / (X['total_claim_amount'] + 1)
    
    if 'capital-gains' in X.columns and 'capital-loss' in X.columns:
        # Net capital
        X['net_capital'] = X['capital-gains'] - X['capital-loss']
    
    # Encode categorical columns
    label_encoders = {}
    cat_cols = X.select_dtypes(include=['object']).columns
    
    for col in cat_cols:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        label_encoders[col] = le
    
    return X, y, label_encoders, list(X.columns)


def train_model(X: pd.DataFrame, y: np.ndarray) -> dict:
    """
    Train XGBoost model with cross-validation.
    """
    logger.info("Training fraud detection model...")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Try XGBoost, fall back to sklearn if not available
    try:
        from xgboost import XGBClassifier
        from sklearn.model_selection import cross_val_score
        
        # Calculate scale_pos_weight for imbalanced data
        scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
        
        # Create ensemble of models
        from sklearn.ensemble import VotingClassifier
        
        # XGBoost with different settings
        xgb_1 = XGBClassifier(
            n_estimators=150,
            max_depth=5,
            learning_rate=0.03,
            scale_pos_weight=scale_pos_weight * 1.3,
            min_child_weight=2,
            subsample=0.8,
            colsample_bytree=0.8,
            gamma=0.1,
            reg_alpha=0.3,
            reg_lambda=1.0,
            random_state=42,
            eval_metric='auc'
        )
        
        xgb_2 = XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            scale_pos_weight=scale_pos_weight * 1.5,
            min_child_weight=3,
            subsample=0.7,
            colsample_bytree=0.7,
            gamma=0.2,
            reg_alpha=0.5,
            reg_lambda=1.5,
            random_state=123,
            eval_metric='auc'
        )
        
        # RandomForest
        from sklearn.ensemble import RandomForestClassifier
        class_weight = {0: 1, 1: scale_pos_weight}
        rf = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            class_weight=class_weight,
            random_state=42,
            n_jobs=-1
        )
        
        # Voting ensemble (soft voting for probability-based)
        model = VotingClassifier(
            estimators=[
                ('xgb1', xgb_1),
                ('xgb2', xgb_2),
                ('rf', rf)
            ],
            voting='soft',
            n_jobs=-1
        )
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='roc_auc')
        logger.info(f"Cross-validation AUC: {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")
        
    except ImportError:
        from sklearn.ensemble import RandomForestClassifier
        logger.warning("Using RandomForest with class_weight")
        
        # Calculate class weight for imbalanced data
        class_weight = {0: 1, 1: (y_train == 0).sum() / (y_train == 1).sum()}
        
        model = RandomForestClassifier(
            n_estimators=150,
            max_depth=12,
            min_samples_split=5,
            min_samples_leaf=2,
            class_weight=class_weight,  # Handle imbalanced data
            random_state=42,
            n_jobs=-1
        )
    
    # Train
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    # Metrics
    accuracy = (y_pred == y_test).mean()
    auc_score = roc_auc_score(y_test, y_proba)
    
    logger.info(f"Model trained. Accuracy: {accuracy:.3f}, AUC: {auc_score:.3f}")
    
    # Feature importance (if available)
    if hasattr(model, 'feature_importances_'):
        importance = pd.DataFrame({
            'feature': X.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        logger.info(f"Top 5 features: {importance.head()['feature'].tolist()}")
    
    results = {
        "model": model,
        "accuracy": accuracy,
        "auc_score": auc_score,
        "X_test": X_test,
        "y_test": y_test,
        "y_pred": y_pred,
        "y_proba": y_proba,
    }
    
    return results


def save_model(model, label_encoders, feature_names):
    """Save trained model and encoders to disk."""
    import os
    os.makedirs("models", exist_ok=True)
    
    joblib.dump(model, MODEL_PATH)
    joblib.dump({'encoders': label_encoders, 'features': feature_names}, ENCODER_PATH)
    logger.info(f"Model saved to {MODEL_PATH}")


def load_model():
    """Load trained model from disk."""
    if not (joblib.os.path.exists(MODEL_PATH) and joblib.os.path.exists(ENCODER_PATH)):
        raise FileNotFoundError("Model not found. Train first with: python fraud_model.py")
    
    model = joblib.load(MODEL_PATH)
    encoder_data = joblib.load(ENCODER_PATH)
    
    return {
        'model': model,
        'encoders': encoder_data['encoders'],
        'features': encoder_data['features']
    }


def predict_fraud(claim_data: dict, model_data: dict = None) -> dict:
    """
    Predict fraud for a single claim.
    
    Args:
        claim_data: dict with claim features
        model_data: loaded model (optional, will load if not provided)
    
    Returns:
        dict with prediction and confidence
    """
    if model_data is None:
        model_data = load_model()
    
    model = model_data['model']
    features = model_data['features']
    
    # Create DataFrame with same columns
    df = pd.DataFrame([claim_data])
    
    # Ensure all feature columns exist
    for col in features:
        if col not in df.columns:
            df[col] = 0
    
    # Select only features the model was trained on
    X = df[features].copy()
    
    # Encode categorical columns
    encoders = model_data.get('encoders', {})
    for col in X.select_dtypes(include=['object']).columns:
        if col in encoders:
            le = encoders[col]
            # Handle unseen labels
            known_labels = set(le.classes_)
            X[col] = X[col].apply(lambda x: x if x in known_labels else le.classes_[0])
            X[col] = le.transform(X[col].astype(str))
    
    # Predict with optimized threshold
    # Lower threshold = higher recall (catch more fraud)
    # Higher threshold = higher precision (fewer false positives)
    FRAUD_THRESHOLD = 0.35  # Lowered to catch more fraud
    
    proba = model.predict_proba(X)[0, 1]
    prediction = int(proba >= FRAUD_THRESHOLD)
    
    # Decision logging - explain WHY prediction was made
    logger.info(f"Prediction made: is_fraud={prediction}, probability={proba:.3f}")
    
    # Get feature importance for this prediction
    if hasattr(model, 'feature_importances_'):
        importances = pd.DataFrame({
            'feature': features,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        top_5_features = importances.head(5)['feature'].tolist()
        logger.info(f"Top 5 influential features: {top_5_features}")
        
        # Log key feature values for fraud indicators
        key_features = {
            'no_witness_injury': X.get('no_witness_injury', [0])[0],
            'claim_to_premium_ratio': X.get('claim_to_premium_ratio', [0])[0],
            'incident_severity': X.get('incident_severity', ['Unknown'])[0],
            'total_claim_amount': X.get('total_claim_amount', [0])[0],
        }
        logger.info(f"Key feature values: {key_features}")
    
    return {
        'is_fraud': prediction,
        'fraud_probability': round(proba, 3),
        'confidence': 'high' if proba > 0.75 or proba < 0.25 else 'medium'
    }


def print_evaluation(results: dict):
    """Pretty print model evaluation."""
    print("\n" + "="*50)
    print("MODEL EVALUATION")
    print("="*50)
    print(f"Accuracy: {results['accuracy']:.3f}")
    print(f"AUC Score: {results['auc_score']:.3f}")
    print("\nClassification Report:")
    print(classification_report(results['y_test'], results['y_pred']))
    print("\nConfusion Matrix:")
    print(confusion_matrix(results['y_test'], results['y_pred']))
    print("="*50 + "\n")


def main():
    """Train and save the model."""
    # Load data
    df = load_data()
    
    # Prepare features
    X, y, encoders, feature_names = prepare_features(df)
    logger.info(f"Training with {X.shape[1]} features, {len(y)} samples")
    
    # Train
    results = train_model(X, y)
    
    # Save
    save_model(results['model'], encoders, feature_names)
    
    # Print evaluation
    print_evaluation(results)
    
    # Test prediction
    sample = X.iloc[0].to_dict()
    prediction = predict_fraud(sample)
    logger.info(f"Sample prediction: {prediction}")


if __name__ == "__main__":
    main()