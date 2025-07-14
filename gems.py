import os, re, time, json, logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class FacebookAdScraper:
    def __init__(self, credentials_file: str):
        self.credentials_file = credentials_file
        self.gc = self.sheet = self.worksheet = None
        self.last_update_col = None
        self.setup_google_sheets()
        self.setup_selenium()

    # ── Google Sheets helpers ───────────────────────────────────────────────
    def col_letter(self, num: int) -> str:
        s = ""
        while num:
            num, rem = divmod(num - 1, 26)
            s = chr(65 + rem) + s
        return s

    def setup_google_sheets(self):
        scope = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = Credentials.from_service_account_file(self.credentials_file, scopes=scope)
        self.gc = gspread.authorize(creds)

        self.sheet = self.gc.open("Window Replacement 2025 Swipe File ")
        self.worksheet = self.sheet.worksheet("Window Replace")

        headers = self.worksheet.row_values(1)
        for i, h in enumerate(headers, 1):
            if h.strip().lower() in {"last update time", "last updated", "updated"}:
                self.last_update_col = i
                break
        else:
            self.last_update_col = len(headers) + 1
            self.worksheet.update_cell(1, self.last_update_col, "Last update time")
        logger.info(f"'Last update time' column: {self.col_letter(self.last_update_col)}")

    # ── Selenium setup ──────────────────────────────────────────────────────
    def setup_selenium(self):
        chrome_opts = Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--window-size=1920,1080")
        chrome_opts.add_argument(
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_opts)

    # ── Core scraping logic ─────────────────────────────────────────────────
    def extract_ad_count(self, url: str) -> int:
        try:
            self.driver.get(url)
            time.sleep(4)  # let FB load
            wait = WebDriverWait(self.driver, 15)

            candidates = [
                '[data-testid="results-count"]',
                'div[role="heading"][aria-level="3"]',
                "h3",
            ]
            results_text = None
            for sel in candidates:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for e in els:
                    txt = e.text.strip()
                    if "result" in txt.lower() and any(ch.isdigit() for ch in txt):
                        results_text = txt
                        break
                if results_text:
                    break

            if not results_text:
                #  fallback – search page source
                matches = re.findall(r"~?(\d+)\s*results?", self.driver.page_source, re.I)
                results_text = matches[0] if matches else ""

            match = re.search(r"~?(\d+)", results_text)
            return int(match.group(1)) if match else 0
        except Exception as exc:
            logger.warning(f"Failed on {url}: {exc}")
            return 0

    def update_sheet(self, row: int, count: int):
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.worksheet.update_cell(row, 5, count)  # Column E
        self.worksheet.update_cell(row, self.last_update_col, ts)

    # ── Public helpers ──────────────────────────────────────────────────────
    def process_all_rows(self):
        rows = self.worksheet.get_all_values()[1:]  # skip header
        total = len(rows)
        updated = skipped = 0

        for idx, row in enumerate(tqdm(rows, desc="Scraping", unit="row"), start=2):
            if len(row) < 2:
                skipped += 1
                continue

            url = row[1]
            if not url.startswith("https://www.facebook.com/ads/library/"):
                skipped += 1
                continue

            count = self.extract_ad_count(url)
            self.update_sheet(idx, count)
            updated += 1
            time.sleep(2)  # polite delay

        logger.info(
            f"Done – {updated} rows updated, {skipped} skipped (Total: {total})"
        )

    def close(self):
        if hasattr(self, "driver"):
            self.driver.quit()


# ─── Main entrypoint ───────────────────────────────────────────────────────
def main():
    creds_path = os.getenv("GOOGLE_CREDS", "credentials.json")
    if not os.path.exists(creds_path):
        logger.error(f"Credentials file not found: {creds_path}")
        return

    scraper = FacebookAdScraper(creds_path)
    try:
        scraper.process_all_rows()
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
