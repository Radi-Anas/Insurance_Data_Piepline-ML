"""
Drift Detection for Fraud Detection Model

Monitors for data drift and concept drift in production.
Alerts when model performance degrades significantly.

Usage:
    from src.pipelines.drift_detection import check_drift
    
    drift_result = check_drift(new_data, model_data)
"""

import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime
from scipy import stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DRIFT_THRESHOLD = 0.05  # 5% drift threshold
CONCEPT_DRIFT_THRESHOLD = 0.10  # 10% accuracy drop


class DriftDetector:
    """Detects data and concept drift in ML pipelines."""
    
    def __init__(self, reference_data: pd.DataFrame, feature_schema: Dict):
        """
        Initialize drift detector with reference data.
        
        Args:
            reference_data: Training data (baseline)
            feature_schema: Expected feature types and ranges
        """
        self.reference_data = reference_data
        self.feature_schema = feature_schema
        self.reference_stats = self._compute_statistics(reference_data)
        self.drift_history = []
        
    def _compute_statistics(self, df: pd.DataFrame) -> Dict:
        """Compute summary statistics for reference data."""
        stats_dict = {}
        
        for col in df.select_dtypes(include=[np.number]).columns:
            stats_dict[col] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'max': df[col].max(),
                'quantiles': df[col].quantile([0.25, 0.5, 0.75]).to_dict()
            }
        
        for col in df.select_dtypes(include=['object']).columns:
            stats_dict[col] = {
                'distribution': df[col].value_counts(normalize=True).to_dict()
            }
        
        return stats_dict
    
    def detect_data_drift(self, new_data: pd.DataFrame) -> Dict:
        """
        Detect feature drift (changes in input data distribution).
        
        Uses Population Stability Index (PSI) and statistical tests.
        """
        drift_results = {
            'timestamp': datetime.now().isoformat(),
            'drifted_features': [],
            'overall_drift': 0.0,
            'severity': 'none'
        }
        
        for col in self.reference_stats.keys():
            if col not in new_data.columns:
                continue
            
            ref_col = self.reference_stats[col]
            
            if 'mean' in ref_col:  # Numeric feature
                # Kolmogorov-Smirnov test
                ks_stat, ks_pvalue = stats.ks_2samp(
                    self.reference_data[col].dropna(),
                    new_data[col].dropna()
                )
                
                # PSI (Population Stability Index)
                psi = self._compute_psi(
                    self.reference_data[col],
                    new_data[col]
                )
                
                if psi > DRIFT_THRESHOLD or ks_pvalue < 0.05:
                    drift_results['drifted_features'].append({
                        'feature': col,
                        'psi': psi,
                        'ks_statistic': ks_stat,
                        'ks_pvalue': ks_pvalue,
                        'drift_detected': True
                    })
                    
            else:  # Categorical feature
                # Chi-square test
                ref_dist = pd.Series(ref_col['distribution'])
                new_dist = new_data[col].value_counts(normalize=True)
                
                # Align categories
                all_cats = list(set(ref_dist.index) | set(new_dist.index))
                ref_aligned = ref_dist.reindex(all_cats, fill_value=0)
                new_aligned = new_dist.reindex(all_cats, fill_value=0)
                
                chi2, chi2_pvalue = stats.chisquare(
                    new_aligned.values,
                    ref_aligned.values
                )
                
                if chi2_pvalue < 0.05:
                    drift_results['drifted_features'].append({
                        'feature': col,
                        'chi2_statistic': chi2,
                        'chi2_pvalue': chi2_pvalue,
                        'drift_detected': True
                    })
        
        # Compute overall drift score
        if drift_results['drifted_features']:
            drift_scores = [f.get('psi', 0) for f in drift_results['drifted_features']]
            drift_results['overall_drift'] = np.mean(drift_scores)
            
            # Determine severity
            if drift_results['overall_drift'] > 0.25:
                drift_results['severity'] = 'critical'
            elif drift_results['overall_drift'] > 0.15:
                drift_results['severity'] = 'high'
            elif drift_results['overall_drift'] > DRIFT_THRESHOLD:
                drift_results['severity'] = 'moderate'
        
        self.drift_history.append(drift_results)
        
        return drift_results
    
    def _compute_psi(self, reference: pd.Series, new: pd.Series, 
                    buckets: int = 10) -> float:
        """
        Compute Population Stability Index.
        
        PSI < 0.1: No significant change
        0.1 <= PSI < 0.2: Minor change
        PSI >= 0.2: Significant change
        """
        # Create buckets from reference
        references = reference.dropna()
        quantiles = np.linspace(0, 100, buckets + 1)
        breaks = np.percentile(references, quantiles)
        breaks[0] = -np.inf
        breaks[-1] = np.inf
        
        # Calculate percentages in each bucket
        ref_pct = np.histogram(reference, bins=breaks)[0] / len(reference)
        new_pct = np.histogram(new, bins=breaks)[0] / len(new)
        
        # Avoid division by zero
        ref_pct = np.where(ref_pct == 0, 0.0001, ref_pct)
        new_pct = np.where(new_pct == 0, 0.0001, new_pct)
        
        # Calculate PSI
        psi = np.sum((new_pct - ref_pct) * np.log(new_pct / ref_pct))
        
        return psi
    
    def detect_prediction_drift(self, new_predictions: List[int],
                            new_probabilities: List[float]) -> Dict:
        """
        Detect concept drift (changes in model's predictions).
        
        Monitors if fraud rate or average probability changes significantly.
        """
        reference_predictions = self.reference_data['is_fraud'].values
        ref_fraud_rate = reference_predictions.mean()
        ref_avg_prob = 0.24  # From training
        
        new_fraud_rate = np.mean(new_predictions)
        new_avg_prob = np.mean(new_probabilities)
        
        fraud_rate_change = abs(new_fraud_rate - ref_fraud_rate)
        prob_change = abs(new_avg_prob - ref_avg_prob)
        
        return {
            'timestamp': datetime.now().isoformat(),
            'reference_fraud_rate': ref_fraud_rate,
            'current_fraud_rate': new_fraud_rate,
            'fraud_rate_change': fraud_rate_change,
            'reference_avg_probability': ref_avg_prob,
            'current_avg_probability': new_avg_prob,
            'probability_change': prob_change,
            'drift_detected': fraud_rate_change > CONCEPT_DRIFT_THRESHOLD or prob_change > 0.1,
            'severity': 'critical' if fraud_rate_change > 0.15 else 
                     'high' if fraud_rate_change > 0.10 else
                     'moderate' if fraud_rate_change > CONCEPT_DRIFT_THRESHOLD else 'none'
        }
    
    def get_drift_report(self) -> Dict:
        """Get comprehensive drift report."""
        if not self.drift_history:
            return {'message': 'No drift analysis performed yet'}
        
        latest = self.drift_history[-1]
        
        return {
            'latest_drift': latest,
            'total_checks': len(self.drift_history),
            'drift_trends': self._analyze_trends(),
            'recommendation': self._get_recommendation(latest)
        }
    
    def _analyze_trends(self) -> Dict:
        """Analyze drift trends over time."""
        if len(self.drift_history) < 2:
            return {'message': 'Insufficient data for trend analysis'}
        
        drift_scores = [h.get('overall_drift', 0) for h in self.drift_history]
        
        return {
            'increasing': drift_scores[-1] > drift_scores[0],
            'mean_drift': np.mean(drift_scores),
            'max_drift': np.max(drift_scores)
        }
    
    def _get_recommendation(self, latest_drift: Dict) -> str:
        """Get actionable recommendation based on drift."""
        severity = latest_drift.get('severity', 'none')
        
        recommendations = {
            'none': 'Model is performing well. Continue monitoring.',
            'moderate': 'Consider retraining model in next cycle.',
            'high': 'Schedule immediate model retraining.',
            'critical': 'URGENT: Retrain model before production use.'
        }
        
        return recommendations.get(severity, 'Unknown')


def check_drift(new_data: pd.DataFrame, model_data: Dict,
              predictions: List[int] = None,
              probabilities: List[float] = None) -> Dict:
    """
    Convenience function to check drift.
    
    Args:
        new_data: New incoming data
        model_data: Loaded model
        predictions: Model predictions for new data
        probabilities: Prediction probabilities
    
    Returns:
        Dict with drift analysis
    """
    from src.models.fraud_model import load_data
    
    # Get reference data
    reference = load_data()
    
    # Initialize detector
    detector = DriftDetector(reference, model_data.get('features', {}))
    
    # Check data drift
    data_drift = detector.detect_data_drift(new_data)
    
    # Check prediction drift if provided
    prediction_drift = {}
    if predictions is not None and probabilities is not None:
        prediction_drift = detector.detect_prediction_drift(predictions, probabilities)
    
    return {
        'data_drift': data_drift,
        'prediction_drift': prediction_drift,
        'action_required': data_drift.get('severity') in ['moderate', 'high', 'critical'] or
                         prediction_drift.get('drift_detected', False)
    }


# Add drift check to API
def add_drift_endpoint(app):
    """Add drift detection endpoint to API."""
    from fastapi import HTTPException
    
    @app.get("/drift/check")
    def check_model_drift():
        """
        Check for data and concept drift.
        
        Returns drift analysis since last model training.
        """
        from src.models.fraud_model import load_data, load_model
        
        try:
            model_data = load_model()
            if model_data is None:
                raise HTTPException(status_code=503, detail="Model not loaded")
            
            new_data = load_data()
            
            predictions = []
            probabilities = []
            
            for _, row in new_data.iterrows():
                result = row.to_dict()
                predictions.append(result.get('is_fraud', 0))
                probabilities.append(result.get('fraud_probability', 0))
            
            drift_result = check_drift(new_data, model_data, predictions, probabilities)
            
            return drift_result
            
        except Exception as e:
            logger.error(f"Drift check failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))