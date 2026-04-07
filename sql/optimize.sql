-- Database optimization: indexes for performance

-- Index on fraud flag for fast filtering
CREATE INDEX IF NOT EXISTS idx_claims_is_fraud ON claims(is_fraud);

-- Index on policy number for lookups
CREATE INDEX IF NOT EXISTS idx_claims_policy_number ON claims(policy_number);

-- Index on auto make for fraud analysis by vehicle
CREATE INDEX IF NOT EXISTS idx_claims_auto_make ON claims(auto_make);

-- Index on incident severity for fraud analysis
CREATE INDEX IF NOT EXISTS idx_claims_incident_severity ON claims(incident_severity);

-- Index on total claim amount for range queries
CREATE INDEX IF NOT EXISTS idx_claims_total_claim_amount ON claims(total_claim_amount);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_claims_fraud_severity ON claims(is_fraud, incident_severity);

-- Index on policy annual premium for financial queries
CREATE INDEX IF NOT EXISTS idx_claims_premium ON claims(policy_annual_premium);

-- Analyze table for query optimization
ANALYZE claims;
