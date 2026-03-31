"""
debug_avito.py
Temporary script to inspect Avito's HTML structure.
Run this once to find the correct CSS selectors, then delete it.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def debug():
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

    try:
        driver.get("https://www.avito.ma/fr/maroc/immobilier?o=1")
        time.sleep(5)  # Wait for full JS render

        # Save the full page HTML so we can inspect it
        with open("avito_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        print("✅ Page saved to avito_debug.html")
        print(f"Page title: {driver.title}")
        print(f"Page source length: {len(driver.page_source)} characters")

        # Print all <a> tags that look like listings
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Show first 5 anchor tags to understand the structure
        all_links = soup.find_all("a", href=True)
        print(f"\nTotal <a> tags found: {len(all_links)}")
        print("\nFirst 10 hrefs:")
        for a in all_links[:10]:
            print(f"  {a.get('href', '')[:80]}")

    finally:
        driver.quit()

if __name__ == "__main__":
    debug()