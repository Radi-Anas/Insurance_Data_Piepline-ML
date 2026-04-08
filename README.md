# Insurance Claims Fraud Detection System

A data engineering pipeline that detects fraudulent insurance claims. Built to solve a real business problem and demonstrate core data engineering skills.

---

## The Problem

Insurance fraud costs companies billions annually. When a claim comes in, investigators must manually review each one - taking hours per claim. This system helps prioritize which claims need immediate attention by scoring them for fraud probability.

**Business Impact:**
- Reduce manual review time by 70%
- Catch more fraud with consistent scoring
- Enable data-driven investigation decisions

---

## Data Flow

```
1. RAW DATA (CSV)
   └── Insurance claims with customer, policy, incident details

2. ETL PIPELINE (claims_etl.py)
   ├── Extract: Load from CSV
   ├── Transform: Clean data, handle missing values, create features
   ├── Validate: Check data quality, enforce schema
   └── Load: Insert into PostgreSQL

3. DATABASE (PostgreSQL)
   └── 1000 claims, indexed on fraud flag, severity, vehicle make
   └── Connection pooling (5 connections, 10 overflow)

4. ML MODEL (fraud_model.py)
   ├── Features: 29 columns (customer info, policy, incident, amounts)
   ├── Target: fraud_reported (Y/N)
   └── Training: RandomForest with balanced class weights

5. API (api.py)
   ├── /predict: Single claim prediction
   ├── /stats: Aggregated fraud statistics
   ├── /health: System health check
   └── Caching: 5-minute TTL on /stats

6. DASHBOARD (dashboard.py)
   ├── Overview: Fraud rate by vehicle, severity charts
   ├── Claims browser with filters
   └── Model performance metrics
```

---

## Model Performance

| Metric | Value | Why It Matters |
|--------|-------|-----------------|
| Accuracy | 81.5% | Overall correctness |
| AUC-ROC | 0.805 | Strong ranking ability |
| Precision | 60% | When we say fraud, 60% actually are fraud |
| Recall | 71% | We catch 71% of actual fraud |

**Improvements:**
- Using ensemble (XGBoost x2 + RandomForest) with voting
- Added feature engineering (8 new derived features)
- Cross-validation for reliable performance estimation
- Features like `no_witness_injury`, `claim_to_premium_ratio`
- Regularization (gamma, reg_alpha, reg_lambda) to prevent overfitting

---

## Tech Stack

| Component | Technology | Version |
|-----------|------------|----------|
| Language | Python | 3.10 |
| Database | PostgreSQL | 15 |
| ML | scikit-learn | 1.5 |
| API | FastAPI | 0.115 |
| Dashboard | Streamlit | 1.40 |
| Scheduling | Prefect | 3.0 |
| Testing | pytest | 8.3 |
| Docker | docker-compose | 3.8 |

---

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL (or use Docker)

### Setup

```bash
# Clone and install
git clone https://github.com/Radi-Anas/Insurance_Data_Piepline-ML.git
cd Insurance_Data_Piepline-ML
pip install -r requirements.txt

# Configure database
cp .env.development .env
# Edit .env with your DB credentials

# Run everything
python main.py
```

### Access
- Dashboard: http://localhost:8501
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Database & model status |
| `/predict` | POST | Score a claim (10/min limit) |
| `/predict/batch` | POST | Batch score claims (5/min limit) |
| `/stats` | GET | Fraud statistics (cached) |
| `/claims` | GET | List claims with filters |
| `/model/metrics` | GET | Model performance (API key required) |

### Example Usage

```python
import requests

response = requests.post("http://localhost:8000/predict", json={
    "months_as_customer": 12,
    "age": 35,
    "policy_state": "OH",
    "policy_annual_premium": 1200,
    "incident_type": "Single Vehicle Collision",
    "incident_severity": "Major Damage",
    "total_claim_amount": 5000,
    "auto_make": "Toyota"
})

print(response.json())
# {"prediction": 1, "fraud_probability": 0.54, "confidence": "medium", "risk_level": "MEDIUM"}
```

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

---

## Docker

```bash
# Start all services
docker-compose up -d

# Services:
# - postgres:5432
# - api:8000
# - dashboard:8501
```

---

## What's Included

### Data Pipeline
- ETL with pandas/SQLAlchemy
- Data validation rules
- Connection pooling
- Database indexes

### Machine Learning
- RandomForest classifier
- Label encoding for categoricals
- Model persistence with joblib

### API
- FastAPI with Pydantic
- Rate limiting (slowapi)
- In-memory caching
- API key authentication

### DevOps
- Docker Compose
- GitHub Actions CI/CD
- 36 unit tests

### Monitoring
- Health check endpoints
- Prometheus metrics endpoint

---

## Project Structure

```
.
├── api.py                 # FastAPI application
├── claims_etl.py          # ETL pipeline
├── fraud_model.py        # ML model training
├── dashboard.py          # Streamlit dashboard
├── main.py               # Orchestration
├── requirements.txt       # Pinned dependencies
│
├── config/               # Settings
├── scripts/             # Backup/restore
├── pipeline/             # Utilities
├── tests/                # Unit tests
└── .github/workflows/   # CI/CD
```

---

## License

MIT
