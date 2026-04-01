"""
pipeline/extract.py

Two-pass scraper for Avito.ma real estate listings.

Pass 1: Collect listing URLs from search result pages (JSON-LD on search pages)
Pass 2: Visit each listing's detail page to extract full data (JSON-LD on detail pages)

This two-pass approach gives us rich data including surface area, rooms,
listing type, and seller type — fields not available on search result pages.
"""

import json
import re
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DRIVER SETUP
# ---------------------------------------------------------------------------

def build_driver() -> webdriver.Chrome:
    """
    Create and return a configured headless Chrome WebDriver.
    Selenium 4.6+ manages ChromeDriver automatically — no manual setup needed.
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


# ---------------------------------------------------------------------------
# PASS 1 — COLLECT LISTING URLS FROM SEARCH PAGES
# ---------------------------------------------------------------------------

def collect_listing_urls(driver: webdriver.Chrome, max_pages: int = 10) -> list:
    """
    Scrape search result pages and collect individual listing URLs.

    Each search page embeds ~38 listings as JSON-LD script tags.
    We extract only the URLs here — full details are scraped in Pass 2.

    Args:
        driver:    Active Chrome WebDriver instance.
        max_pages: Number of search pages to scrape.

    Returns:
        List of unique listing URLs.
    """
    base_url  = "https://www.avito.ma/fr/maroc/immobilier"
    all_urls  = []
    seen_urls = set()

    for page in range(1, max_pages + 1):
        url = f"{base_url}?o={page}"
        logger.info(f"[Pass 1] Page {page}/{max_pages}: {url}")

        try:
            driver.get(url)
            time.sleep(3)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Avito embeds listing data as JSON-LD with id="search-ad-schema-N"
            script_tags = soup.find_all(
                "script",
                id=lambda i: i and i.startswith("search-ad-schema-")
            )

            if not script_tags:
                logger.warning(f"[Pass 1] No listings found on page {page}. Stopping.")
                break

            page_count = 0
            for script in script_tags:
                try:
                    data        = json.loads(script.string)
                    listing_url = (
                        data.get("url") or
                        data.get("offers", {}).get("url")
                    )
                    if listing_url and listing_url not in seen_urls:
                        seen_urls.add(listing_url)
                        all_urls.append(listing_url)
                        page_count += 1
                except (json.JSONDecodeError, AttributeError):
                    continue

            logger.info(f"[Pass 1] Page {page} done — {page_count} URLs. "
                        f"Total: {len(all_urls)}")

        except Exception as e:
            logger.error(f"[Pass 1] Error on page {page}: {e}")
            continue

        time.sleep(2)

    logger.info(f"[Pass 1] Complete — {len(all_urls)} unique listing URLs collected.")
    return all_urls


# ---------------------------------------------------------------------------
# PASS 2 — SCRAPE EACH LISTING DETAIL PAGE
# ---------------------------------------------------------------------------

def scrape_listing_detail(driver: webdriver.Chrome, url: str) -> dict:
    """
    Visit a single listing page and extract full details from its JSON-LD.

    Avito detail pages contain two JSON-LD blocks:
      - Script 0: BreadcrumbList (navigation — skip this)
      - Script 1: Apartment / Product (actual listing data — use this)

    Args:
        driver: Active Chrome WebDriver instance.
        url:    Full URL of the listing detail page.

    Returns:
        A dict with all extracted fields, or None if extraction fails.
    """
    try:
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find the listing JSON-LD block — skip BreadcrumbList
        data = None
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                parsed = json.loads(script.string)
                # BreadcrumbList is navigation data — not what we want
                if parsed.get("@type") != "BreadcrumbList":
                    data = parsed
                    break
            except (json.JSONDecodeError, AttributeError):
                continue

        if not data:
            return None

        # --- Basic fields from JSON-LD ---
        title       = data.get("name")
        description = data.get("description", "")[:300]
        price       = data.get("offers", {}).get("price")
        currency    = data.get("offers", {}).get("priceCurrency", "DH")
        seller_name = data.get("offers", {}).get("seller", {}).get("name")

        # Skip listings with no title or price — not worth storing
        if not title or price is None:
            return None

        # --- Parse city and category from URL ---
        # URL structure: https://www.avito.ma/fr/{city}/{category}/{title}.htm
        city     = None
        category = None
        parts    = url.replace("https://www.avito.ma/", "").strip("/").split("/")
        if len(parts) >= 3:
            city     = parts[1].replace("_", " ").title()
            category = parts[2].replace("_", " ").title()

        # --- Detect listing type (sale vs rental) ---
        text_to_check = f"{title} {description}".lower()
        if any(w in text_to_check for w in ["louer", "location", "loue", "à louer"]):
            listing_type = "Location"
        else:
            listing_type = "Vente"

        # --- Detect seller type (agency vs private) ---
        if seller_name:
            seller_type = (
                "Particulier"
                if seller_name.lower() == "particulier"
                else "Agence"
            )
        else:
            seller_type = None

        # --- Extract surface area and rooms from description text ---
        # Avito doesn't always expose these in JSON — we parse the description
        surface_m2 = extract_surface_from_text(description)
        rooms      = extract_rooms_from_text(description)

        return {
            "title":        title,
            "description":  description,
            "price":        price,
            "currency":     currency,
            "surface_m2":   surface_m2,
            "rooms":        rooms,
            "listing_type": listing_type,
            "seller_name":  seller_name,
            "seller_type":  seller_type,
            "city":         city,
            "category":     category,
            "url":          url,
        }

    except Exception as e:
        logger.debug(f"[Pass 2] Failed to scrape {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# TEXT EXTRACTION HELPERS
# ---------------------------------------------------------------------------

def extract_surface_from_text(text: str):
    """
    Extract surface area in m² from a listing description.

    Handles common Avito patterns:
        "superficie de 85m²"  /  "85 m2"  /  "85m²"  /  "85 mètres carrés"
    """
    if not text:
        return None

    patterns = [
        r"(\d+)\s*m[²2]",
        r"superficie\s*(?:de|:)?\s*(\d+)",
        r"surface\s*(?:de|:)?\s*(\d+)",
        r"(\d+)\s*mètres?\s*carrés?",
    ]

    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            value = int(match.group(1))
            if 10 <= value <= 10_000:   # Sanity check
                return value

    return None


def extract_rooms_from_text(text: str):
    """
    Extract number of rooms from a listing description.

    Handles common Avito patterns:
        "3 chambres"  /  "4 pièces"  /  "F3"
    """
    if not text:
        return None

    patterns = [
        r"(\d+)\s*chambres?",
        r"(\d+)\s*pièces?",
        r"\bF(\d+)\b",
        r"(\d+)\s*rooms?",
    ]

    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            value = int(match.group(1))
            if 1 <= value <= 20:        # Sanity check
                return value

    return None


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATOR
# ---------------------------------------------------------------------------

def scrape_avito(max_pages: int = 10) -> pd.DataFrame:
    """
    Full two-pass scraper for Avito.ma real estate listings.

    Pass 1: Collect ~38 listing URLs per search page
    Pass 2: Visit each URL and extract full listing details

    Args:
        max_pages: Number of search pages to process.
                   10 pages ≈ 300 listings ≈ 2 minutes.

    Returns:
        Raw DataFrame with all scraped listings and full details.
    """
    driver       = build_driver()
    all_listings = []

    try:
        # --- Pass 1 ---
        listing_urls = collect_listing_urls(driver, max_pages=max_pages)

        if not listing_urls:
            logger.error("No URLs collected in Pass 1. Aborting.")
            return pd.DataFrame()

        # --- Pass 2 ---
        total = len(listing_urls)
        logger.info(f"[Pass 2] Scraping details for {total} listings...")

        for i, url in enumerate(listing_urls, 1):
            listing = scrape_listing_detail(driver, url)

            if listing:
                all_listings.append(listing)

            # Progress log every 25 listings
            if i % 25 == 0:
                success_rate = round(len(all_listings) / i * 100)
                logger.info(f"[Pass 2] {i}/{total} processed — "
                            f"{len(all_listings)} successful ({success_rate}% success rate).")

            time.sleep(1.5)     # Polite delay between requests

    finally:
        driver.quit()           # Always close browser even if error occurs

    if not all_listings:
        logger.error("No listings scraped successfully.")
        return pd.DataFrame()

    df = pd.DataFrame(all_listings)
    df = df.drop_duplicates(subset=["url"])
    logger.info(f"[Pass 2] Complete — {len(df)} listings with full details.")
    return df


# ---------------------------------------------------------------------------
# CSV FALLBACK
# ---------------------------------------------------------------------------

def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Fallback: load data from a CSV file instead of scraping.
    Useful for testing transform/load without hitting the website.
    Set USE_SCRAPER = False in main.py to use this mode.
    """
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    logger.info(f"Extracted {len(df)} rows from {file_path}")
    return df