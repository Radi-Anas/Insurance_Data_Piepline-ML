#  Moroccan Real Estate Data Pipeline

An end-to-end ETL (Extract, Transform, Load) data pipeline built with Python and PostgreSQL,
using Moroccan real estate listings as the dataset.

Built as a portfolio project to demonstrate real-world data engineering skills.

---

##  Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3 | Core language |
| pandas | Data cleaning & transformation |
| SQLAlchemy | Database connection (ORM) |
| psycopg2 | PostgreSQL driver |
| PostgreSQL | Data warehouse |
| python-dotenv | Secure credential management |

---

##  Project Structure
```
morocco_re_pipeline/
│   main.py              # Pipeline entry point
│   .env                 # DB credentials (not committed)
│   requirements.txt     # Python dependencies
│
├───pipeline/
│       extract.py       # Load raw CSV data
│       transform.py     # Clean & standardize data
│       load.py          # Write to PostgreSQL
│
├───data/
│   ├───raw/             # Original unmodified data
│   └───clean/           # Cleaned data (post-transform)
│
├───config/
│       settings.py      # Centralized configuration
│
└───sql/
        analysis.sql     # Business analysis queries
```

---

##  How to Run

**1. Clone the repo**
```bash
git clone https://github.com/yourusername/morocco_re_pipeline.git
cd morocco_re_pipeline
```

**2. Create and activate virtual environment**
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure environment variables**

Create a `.env` file in the project root:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=morocco_re
DB_USER=postgres
DB_PASSWORD=yourpassword
```

**5. Create the database**
```sql
CREATE DATABASE morocco_re;
```

**6. Run the pipeline**
```bash
python main.py
```

---

##  Pipeline Steps
```
Raw CSV → extract.py → transform.py → load.py → PostgreSQL
```

| Step | Script | What it does |
|------|--------|-------------|
| Extract | `extract.py` | Reads raw CSV into a DataFrame |
| Transform | `transform.py` | Cleans prices, drops invalid rows, adds `price_per_m2` |
| Load | `load.py` | Writes clean data to PostgreSQL |

---

##  Data Cleaning Rules

- Rows with missing or non-numeric prices are dropped
- Rows with zero surface area are dropped
- City and neighborhood names are standardized (title case)
- A `price_per_m2` column is derived automatically

---

##  Sample SQL Analysis
```sql
-- Average price per city
SELECT city, ROUND(AVG(price), 0) AS avg_price
FROM listings
GROUP BY city
ORDER BY avg_price DESC;
```

---

##  Roadmap

- [x] CSV extraction
- [x] Data cleaning with pandas
- [x] PostgreSQL loading
- [x] SQL analysis queries
- [ ] Web scraping (replace CSV with live data)
- [ ] Scheduled pipeline runs
- [ ] Dashboard visualization

---

##  Author

**Radi Anas**
[LinkedIn](https://linkedin.com/in/https://www.linkedin.com/in/radi-anas/) • [GitHub](https://github.com/Radi-Anas)