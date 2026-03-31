"""
pipeline/extract.py
Scrapes real estate listings from Avito.ma using Selenium.
Avito embeds listing data as JSON-LD in <script> tags —
we parse those directly instead of scraping HTML elements.
This gives us clean, structured data without fragile CSS selectors.
"""

import json
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_driver() -> webdriver.Chrome:
    """
    Create and return a configured Chrome WebDriver.
    Selenium 4.6+ manages ChromeDriver automatically.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    return driver


def scrape_avito(max_pages: int = 3) -> pd.DataFrame:
    """
    Scrape real estate listings from Avito.ma.
    Parses JSON-LD script tags which contain clean structured listing data.

    Args:
        max_pages: Number of pages to scrape (~30 listings per page).

    Returns:
        A raw DataFrame with all scraped listings.
    """
    base_url = "https://www.avito.ma/fr/maroc/immobilier"
    all_listings = []
    driver = build_driver()

    try:
        for page in range(1, max_pages + 1):
            url = f"{base_url}?o={page}"
            logger.info(f"Scraping page {page}: {url}")

            driver.get(url)
            time.sleep(4)  # Wait for JS to fully render

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Find all JSON-LD script tags — Avito uses id="search-ad-schema-N"
            script_tags = soup.find_all(
                "script",
                id=lambda i: i and i.startswith("search-ad-schema-")
            )

            if not script_tags:
                logger.warning(f"No JSON-LD listing data found on page {page}.")
                break

            for script in script_tags:
                listing = extract_listing_data(script)
                if listing:
                    all_listings.append(listing)

            logger.info(f"Page {page} done. Total collected so far: {len(all_listings)}")
            time.sleep(2)

    finally:
        driver.quit()

    if not all_listings:
        logger.error("No listings were scraped.")
        return pd.DataFrame()

    df = pd.DataFrame(all_listings)
    df = df.drop_duplicates(subset=["url"])
    logger.info(f"Scraping complete. Total raw listings: {len(df)}")
    return df


def extract_listing_data(script) -> dict:
    """
    Parse a single JSON-LD script tag into a structured listing dict.

    The JSON structure looks like:
    {
        "name": "Appartement à vendre...",
        "url": "https://www.avito.ma/fr/city/category/...",
        "offers": {
            "price": 950000,
            "priceCurrency": "DH"
        }
    }
    """
    try:
        data = json.loads(script.string)

        title    = data.get("name", None)
        url      = data.get("url") or data.get("offers", {}).get("url")
        price    = data.get("offers", {}).get("price")
        currency = data.get("offers", {}).get("priceCurrency", "DH")
        seller   = data.get("offers", {}).get("seller", {}).get("name", None)

        # Parse city and category from the URL
        # URL structure: https://www.avito.ma/fr/{city}/{category}/{title}.htm
        city     = None
        category = None
        if url:
            parts = url.replace("https://www.avito.ma/", "").strip("/").split("/")
            # parts = ['fr', 'city', 'category', 'title.htm']
            city     = parts[1].replace("_", " ").title() if len(parts) > 1 else None
            category = parts[2].replace("_", " ").title() if len(parts) > 2 else None

        # Skip listings with no title or no URL
        if not title or not url:
            return None

        return {
            "title":    title,
            "price":    price,
            "currency": currency,
            "city":     city,
            "category": category,
            "seller":   seller,
            "url":      url,
        }

    except (json.JSONDecodeError, AttributeError):
        return None


def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Fallback: load data from a CSV file instead of scraping.
    """
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    logger.info(f"Extracted {len(df)} rows from {file_path}")
    return df