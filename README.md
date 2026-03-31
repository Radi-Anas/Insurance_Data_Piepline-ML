# Moroccan Real Estate Data Pipeline

An end-to-end ETL pipeline that scrapes live property listings from Avito.ma,
cleans the data with pandas, and loads it into PostgreSQL for analysis.

Built as a portfolio project to demonstrate practical data engineering skills
on a real-world Moroccan dataset.

---

## Pipeline Architecture
```
┌─────────────────────────────────────────────────┐
│                  EXTRACT                        │
│  Mode A: Selenium scraper → Avito.ma (live)     │
│  Mode B: CSV file → data/raw/listings.csv       │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│                 TRANSFORM                       │
│  pandas: clean prices, drop invalid rows,       │
│  standardize cities, add price_per_m2           │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│                   LOAD                          │
│  SQLAlchemy → PostgreSQL (listings table)       │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│                 ANALYZE                         │
│  SQL queries: avg price by city, type,          │
│  affordability ranking, monthly trends          │
└─────────────────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3 | Core language |
| Selenium | Browser automation for live scraping |
| BeautifulSoup4 | HTML parsing |
| pandas | Data cleaning and transformation |
| SQLAlchemy | Database connection layer |
| psycopg2 | PostgreSQL driver |
| PostgreSQL | Data warehouse |
| python-dotenv | Secure credential management |

---

## Project Structure
```
morocco_re_pipeline/
│   main.py                 # Pipeline entry point
│   .env                    # DB credentials (not committed)
│   requirements.txt        # Python dependencies
│
├───pipeline/
│       extract.py          # Scraper (Selenium) + CSV fallback
│       transform.py        # Data cleaning and standardization
│       load.py             # PostgreSQL loader
│
├───data/
│   ├───raw/
│   │       listings.csv    # Sample dataset (10 listings)
│   └───clean/              # Output of transform step
│
├───config/
│       settings.py         # Centralized DB configuration
│
└───sql/
        analysis.sql        # Business analysis queries
```

---

## Two Ways to Run

### Mode A — Live scraper (recommended)

Scrapes real listings directly from Avito.ma using Selenium.
Requires Google Chrome to be installed.

In `main.py`, set:
```python
USE_SCRAPER = True
```

Then run:
```bash
python main.py
```

Each run collects ~30 listings per page. Adjust the number of pages:
```python
raw_df = scrape_avito(max_pages=5)  # ~150 listings
```

---

### Mode B — CSV mode (no browser needed)

Uses the included sample dataset. Useful for testing the
transform and load steps without hitting the website.

In `main.py`, set:
```python
USE_SCRAPER = False
```

Then run:
```bash
python main.py
```

---

## Setup Instructions

**1. Clone the repo**
```bash
git clone https://github.com/Radi-Anas/morocco_re_pipeline.git
cd morocco_re_pipeline
```

**2. Create and activate a virtual environment**
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

**5. Create the PostgreSQL database**
```sql
CREATE DATABASE morocco_re;
```

**6. Run the pipeline**
```bash
python main.py
```

---

## Data Cleaning Rules

| Rule | Detail |
|------|--------|
| Invalid prices dropped | Non-numeric and missing prices removed |
| Zero surface area dropped | Listings with 0 m² removed |
| Text fields standardized | City, neighborhood, type → title case |
| Duplicates removed | Deduplicated by URL (scraper mode) |
| Price per m² derived | `price_per_m2 = price / surface_m2` |

---

## Sample SQL Analysis
```sql
-- Average price and price per m² by city
SELECT
    city,
    COUNT(*)                    AS listings,
    ROUND(AVG(price), 0)        AS avg_price,
    ROUND(AVG(price_per_m2), 0) AS avg_price_per_m2
FROM listings
WHERE price > 0
GROUP BY city
ORDER BY avg_price DESC;

-- Most affordable listings by price per m²
SELECT title, city, price, surface_m2, price_per_m2
FROM listings
WHERE price > 0
ORDER BY price_per_m2 ASC
LIMIT 10;
```

---

## Sample Output
```
2026-03-31 11:51:52 [INFO] === Pipeline started ===
2026-03-31 11:51:54 [INFO] Scraping page 1: https://www.avito.ma/fr/maroc/immobilier?o=1
2026-03-31 11:52:01 [INFO] Page 1 done. Total collected so far: 38
2026-03-31 11:52:09 [INFO] Page 2 done. Total collected so far: 76
2026-03-31 11:52:17 [INFO] Page 3 done. Total collected so far: 114
2026-03-31 11:52:22 [INFO] Scraping complete. Total raw listings: 113
2026-03-31 11:52:22 [INFO] Transformation complete. 113 clean rows.
2026-03-31 11:52:22 [INFO] Loaded 113 rows into table 'listings'.
2026-03-31 11:52:22 [INFO] === Pipeline finished successfully ===
```

---

## Roadmap

- [x] CSV extraction mode
- [x] Data cleaning with pandas
- [x] PostgreSQL loading with SQLAlchemy
- [x] SQL analysis queries
- [x] Live web scraping with Selenium (Avito.ma)
- [ ] Scheduled daily runs (Task Scheduler / cron)
- [ ] Data quality validation layer
- [ ] Dashboard visualization (Metabase / Grafana)
- [ ] Airflow orchestration
- [ ] dbt transformations

---

## Author

**Radi Anas**
[LinkedIn](https://www.linkedin.com/in/radi-anas/) • [GitHub](https://github.com/Radi-Anas) • [Mail](Anasradi556@gmail.com) 