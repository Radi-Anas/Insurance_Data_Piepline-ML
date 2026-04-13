"""
Feature Store for Fraud Detection

Stores and serves precomputed features for ML models.
Uses Redis for fast lookups in production.

Usage:
    from src.pipelines.feature_store import FeatureStore
    
    store = FeatureStore()
    features = store.get_features(policy_number)
    store.store_features(features)
"""

import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FEATURE_CACHE_TTL = 3600  # 1 hour


class FeatureStore:
    """Redis-backed feature store for ML features."""
    
    def __init__(self, redis_url: str = None):
        """
        Initialize feature store with Redis.
        
        Args:
            redis_url: Redis connection string
        """
        self.redis_client = None
        self.enabled = False
        
        if redis_url is None:
            try:
                from configs.settings import REDIS_URL
                redis_url = REDIS_URL
            except:
                pass
        
        if redis_url:
            try:
                import redis
                self.redis_client = redis.from_url(redis_url)
                self.redis_client.ping()
                self.enabled = True
                logger.info("Feature store enabled with Redis")
            except Exception as e:
                logger.warning(f"Redis not available: {e}. Using in-memory fallback.")
                self._memory_store = {}
        else:
            logger.info("Feature store using in-memory fallback")
            self._memory_store = {}
    
    def _make_key(self, entity_id: str, feature_group: str = "claims") -> str:
        """Generate cache key."""
        return f"feature_store:{feature_group}:{entity_id}"
    
    def store_features(self, entity_id: str, features: Dict,
                   feature_group: str = "claims",
                   ttl: int = FEATURE_CACHE_TTL) -> bool:
        """
        Store features for an entity.
        
        Args:
            entity_id: Unique identifier (e.g., policy_number)
            features: Feature dictionary
            feature_group: Feature group name
            ttl: Time to live in seconds
        
        Returns:
            True if stored successfully
        """
        key = self._make_key(entity_id, feature_group)
        features['_stored_at'] = datetime.now().isoformat()
        
        try:
            if self.enabled and self.redis_client:
                self.redis_client.setex(
                    key, ttl, json.dumps(features)
                )
            else:
                self._memory_store[key] = features
            
            logger.debug(f"Stored features for {entity_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            return False
    
    def get_features(self, entity_id: str, 
                  feature_group: str = "claims") -> Optional[Dict]:
        """
        Retrieve features for an entity.
        
        Args:
            entity_id: Unique identifier
            feature_group: Feature group name
        
        Returns:
            Feature dictionary or None
        """
        key = self._make_key(entity_id, feature_group)
        
        try:
            if self.enabled and self.redis_client:
                data = self.redis_client.get(key)
                if data:
                    return json.loads(data)
            else:
                return self._memory_store.get(key)
            
            return None
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            return None
    
    def delete_features(self, entity_id: str,
                   feature_group: str = "claims") -> bool:
        """Delete features for an entity."""
        key = self._make_key(entity_id, feature_group)
        
        try:
            if self.enabled and self.redis_client:
                self.redis_client.delete(key)
            else:
                self._memory_store.pop(key, None)
            return True
        except Exception as e:
            logger.error(f"Failed to delete features: {e}")
            return False
    
    def compute_and_store_features(self, claim_data: Dict,
                                  entity_id: str = None) -> Dict:
        """
        Compute engineered features and store them.
        
        Args:
            claim_data: Raw claim data
            entity_id: Entity ID (generated if not provided)
        
        Returns:
            Computed features
        """
        if entity_id is None:
            entity_id = claim_data.get('policy_number', 
                                   hashlib.md5(str(claim_data).encode()).hexdigest()[:8])
        
        features = self._compute_claim_features(claim_data)
        self.store_features(entity_id, features)
        
        return features
    
    def _compute_claim_features(self, claim_data: Dict) -> Dict:
        """Compute engineered features for a claim."""
        
        # Claim ratios
        claim_amount = claim_data.get('total_claim_amount', 0)
        premium = claim_data.get('policy_annual_premium', 1)
        
        features = {
            'claim_to_premium_ratio': claim_amount / (premium + 1),
            'vehicle_property_ratio': (
                claim_data.get('vehicle_claim', 0) / 
                (claim_data.get('property_claim', 1) + 1)
            ),
            'injury_ratio': (
                claim_data.get('injury_claim', 0) / 
                (claim_amount + 1)
            ),
        }
        
        # Customer features
        tenure = claim_data.get('months_as_customer', 0)
        age = claim_data.get('age', 1)
        
        features.update({
            'tenure_age_ratio': tenure / (age * 12 + 1),
            'is_new_customer': int(tenure < 12),
            'is_high_value_claim': int(claim_amount > 20000),
        })
        
        # Fraud indicator features
        bodily_injuries = claim_data.get('bodily_injuries', 0)
        witnesses = claim_data.get('witnesses', 0)
        
        features.update({
            'no_witness_injury': int(bodily_injuries > 0 and witnesses == 0),
            'complex_no_witness': int(
                claim_data.get('number_of_vehicles_involved', 1) > 1 and witnesses == 0
            ),
            'deductible_claim_ratio': (
                claim_data.get('policy_deductable', 0) / 
                (claim_amount + 1)
            ),
        })
        
        # Financial stress indicator
        features['net_capital'] = (
            claim_data.get('capital-gains', 0) - 
            claim_data.get('capital-loss', 0)
        )
        
        features['_claim_data'] = claim_data
        
        return features
    
    def batch_store(self, claims: List[Dict]) -> Dict:
        """
        Store features for multiple claims.
        
        Returns:
            Summary of storage operation
        """
        stored = 0
        failed = 0
        
        for claim in claims:
            entity_id = claim.get('policy_number')
            if entity_id:
                if self.compute_and_store_features(claim, entity_id):
                    stored += 1
                else:
                    failed += 1
            else:
                failed += 1
        
        return {
            'stored': stored,
            'failed': failed,
            'total': len(claims)
        }
    
    def get_batch_features(self, entity_ids: List[str],
                       feature_group: str = "claims") -> Dict[str, Dict]:
        """Get features for multiple entities."""
        return {
            eid: self.get_features(eid, feature_group)
            for eid in entity_ids
        }
    
    def get_stats(self) -> Dict:
        """Get feature store statistics."""
        try:
            if self.enabled and self.redis_client:
                info = self.redis_client.info('memory')
                return {
                    'backend': 'redis',
                    'used_memory': info.get('used_memory_human', 'unknown'),
                    'connected': True
                }
            else:
                return {
                    'backend': 'memory',
                    'stored_features': len(self._memory_store),
                    'connected': True
                }
        except Exception as e:
            return {'error': str(e)}


# Global feature store instance
_feature_store = None


def get_feature_store() -> FeatureStore:
    """Get global feature store instance."""
    global _feature_store
    if _feature_store is None:
        _feature_store = FeatureStore()
    return _feature_store


def add_feature_store_endpoints(app):
    """Add feature store endpoints to API."""
    
    @app.get("/features/{policy_number}")
    def get_policy_features(policy_number: str):
        """Get cached features for a policy."""
        store = get_feature_store()
        features = store.get_features(policy_number)
        
        if features is None:
            return {"message": "No cached features found", "features": {}}
        
        return {"policy_number": policy_number, "features": features}
    
    @app.post("/features/cache")
    def cache_claim_features(claim_data: dict):
        """Cache features for a claim."""
        store = get_feature_store()
        
        features = store.compute_and_store_features(claim_data)
        
        return {
            "status": "cached",
            "features_stored": len(features) - 1  # Exclude raw data
        }
    
    @app.get("/features/stats")
    def get_feature_store_stats():
        """Get feature store statistics."""
        store = get_feature_store()
        return store.get_stats()