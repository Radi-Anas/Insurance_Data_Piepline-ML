-- sql/analysis.sql
-- Insurance Claims Fraud Analysis Queries
-- Run these in pgAdmin, DBeaver, or psql after the pipeline loads data.

-- 1. Overall fraud statistics
SELECT 
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_percent
FROM claims;

-- 2. Fraud rate by incident severity
SELECT 
    incident_severity,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY incident_severity
ORDER BY fraud_rate DESC;

-- 3. Fraud rate by policy state
SELECT 
    policy_state,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(total_claim_amount)::NUMERIC, 2) AS avg_claim_amount
FROM claims
GROUP BY policy_state
ORDER BY fraud_count DESC;

-- 4. Top 5 vehicle makes with highest fraud rate
SELECT 
    auto_make,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY auto_make
HAVING COUNT(*) > 50
ORDER BY fraud_rate DESC
LIMIT 5;

-- 5. Fraud by customer tenure
SELECT 
    tenure_group,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY tenure_group
ORDER BY fraud_rate DESC;

-- 6. Average claim amount: fraud vs non-fraud
SELECT 
    is_fraud,
    COUNT(*) AS claim_count,
    ROUND(AVG(total_claim_amount)::NUMERIC, 2) AS avg_claim_amount,
    ROUND(AVG(policy_annual_premium)::NUMERIC, 2) AS avg_premium,
    ROUND(AVG(months_as_customer)::NUMERIC, 2) AS avg_tenure_months
FROM claims
GROUP BY is_fraud;

-- 7. Claims with no witnesses (high fraud risk indicator)
SELECT 
    CASE WHEN witnesses = 0 THEN 'No Witnesses' ELSE 'Has Witnesses' END AS witness_status,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY CASE WHEN witnesses = 0 THEN 'No Witnesses' ELSE 'Has Witnesses' END;

-- 8. Age group fraud analysis
SELECT 
    age_group,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY age_group
ORDER BY fraud_rate DESC;

-- 9. Incident type breakdown
SELECT 
    incident_type,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(total_claim_amount)::NUMERIC, 2) AS avg_claim_amount
FROM claims
GROUP BY incident_type
ORDER BY fraud_count DESC;

-- 10. Bodily injuries correlation with fraud
SELECT 
    bodily_injuries,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM claims
GROUP BY bodily_injuries
ORDER BY bodily_injuries;
