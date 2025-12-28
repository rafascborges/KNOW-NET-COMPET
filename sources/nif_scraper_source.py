import re
from pathlib import Path
from typing import Optional, TypedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from elt_core.base_source import BaseDataSource
from sources.lookups.regex_postal_district import REGEX_POSTAL_DISTRICT

POSTAL_RE = re.compile(r"\b\d{4}-\d{0,3}")
QUEUE_DB = "nifs_scrape_queue"
TARGET_DB = "nifs_scrape_bronze"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
}

class ScrapeResult(TypedDict):
    _id: str
    nif: str
    valid_nif: Optional[bool]
    postal_code: Optional[str]
    district: Optional[str]
    description: Optional[str]


def get_district_from_postal(postal_code: Optional[str]) -> Optional[str]:
    """Map a Portuguese postal code to its district using shared lookup data."""
    if not postal_code:
        return None
    for pattern, district in REGEX_POSTAL_DISTRICT.items():
        if re.match(pattern, postal_code):
            return district
    return None

def _is_valid_nif_format(nif: str) -> bool:
    """Return True if the supplied string looks like a 9-digit numeric NIF."""
    return bool(re.fullmatch(r"\d{9}", nif))



class NifScraperSource(BaseDataSource):
    source_name = "nif_scrape"
    """
    Scrapes NIF data from nif.pt.

    Dependencies:
        - Expects a queue database (default: 'nifs_scrape_queue') containing documents with a 'nif' field.
        - Writes results to a bronze database (default: 'nifs_scrape_bronze').
    """
    def __init__(self, db_connector):
        super().__init__(db_connector=db_connector, file_path=None)
        
        # Initialize session with retry strategy
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def run(self, batch_size: int = 5000, queue_db_name: str = QUEUE_DB, target_db_name: str = TARGET_DB, max_workers: int = 10) -> None:
        """
        Reads NIFs from the queue, scrapes data, and saves to bronze in batches.

        Args:
            batch_size (int): Number of records to process per batch.
            queue_db_name (str): Name of the database to read NIFs from.
            target_db_name (str): Name of the database to save scraped results to.
            max_workers (int): Maximum number of concurrent threads.
        """
        self.logger.info("Starting NIF Scraper Source...")
        
        # 1. Fetch pending NIFs from the queue
        queue_db = queue_db_name
        try:
            queue_docs = self.db_connector.get_all_documents(queue_db)
        except Exception as e:
            self.logger.warning(f"Could not fetch docs from {queue_db}. It might not exist yet. Error: {e}")
            return

        total_queue_docs = len(queue_docs)
        self.logger.info(f"Found {total_queue_docs} documents in {queue_db}.")
        
        # 2. Fetch already scraped NIFs from bronze to avoid re-scraping
        target_db = target_db_name
        try:
            bronze_docs = self.db_connector.get_all_documents(target_db)
            scraped_nifs = {doc.get('nif') for doc in bronze_docs if doc.get('nif')}
            self.logger.info(f"Found {len(scraped_nifs)} already scraped NIFs in {target_db}.")
        except Exception as e:
            self.logger.warning(f"Could not fetch docs from {target_db}. Assuming empty. Error: {e}")
            scraped_nifs = set()

        # 3. Filter queue to only include NIFs not yet scraped
        docs_to_scrape = [doc for doc in queue_docs if doc.get('nif') not in scraped_nifs]
        total_to_scrape = len(docs_to_scrape)
        self.logger.info(f"NIFs to scrape after filtering: {total_to_scrape} (skipped {total_queue_docs - total_to_scrape})")
        
        scraped_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_nif = {
                executor.submit(self.scrape, doc.get('nif'), doc.get('description')): doc.get('nif') 
                for doc in docs_to_scrape 
                if doc.get('nif')
            }
            
            self.logger.info(f"Submitted {len(future_to_nif)} scraping tasks with {max_workers} workers.")

            for future in as_completed(future_to_nif):
                nif = future_to_nif[future]
                try:
                    data = future.result()
                    if data:
                        # Add metadata
                        data['_id'] = str(nif) # Use NIF as ID for the bronze doc
                        data['nif'] = nif
                        scraped_results.append(data)
                except Exception as exc:
                    self.logger.error(f"NIF {nif} generated an exception: {exc}")
                
                # Check if batch size reached
                if len(scraped_results) >= batch_size:
                    self.logger.info(f"Saving batch of {len(scraped_results)} scraped records to {target_db}...")
                    self._save_in_batches(scraped_results, target_db)
                    scraped_results = [] # Reset batch

        # Save any remaining results
        if scraped_results:
            self.logger.info(f"Saving final batch of {len(scraped_results)} scraped records to {target_db}...")
            self._save_in_batches(scraped_results, target_db)

        self.logger.info("NIF Scraper finished.")

    def scrape(self, nif: str, description: Optional[str] = None) -> ScrapeResult:
        """
        Scrapes NIF data from nif.pt.
        
        Args:
            nif (str): The NIF to scrape.
            description (str, optional): The entity description/name.
            
        Returns:
            ScrapeResult: The scraped data, or a result with None values if failed.
        """
        nif_str = str(nif).strip()
        
        # 1. Validate Format
        if not _is_valid_nif_format(nif_str):
            self.logger.warning(f"[SKIP] NIF format invalid: {nif_str}")
            return self._create_outcome(nif_str, valid_nif=False, description=description)

        # 2. Fetch HTML
        html_content = self._fetch_html(nif_str)
        if not html_content:
            return self._create_outcome(nif_str, valid_nif=None, description=description)

        # 3. Parse HTML
        return self._parse_html(nif_str, html_content, description)

    def _fetch_html(self, nif_str: str) -> Optional[str]:
        """Fetches the HTML content for a given NIF."""
        url = f"https://www.nif.pt/?q={nif_str}"
        self.logger.info(f"[START] Scraping NIF: {nif_str}")

        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"[ERROR] Failed to scrape NIF: {nif_str}. Error: {e}")
            return None

    def _parse_html(self, nif_str: str, html_content: str, description: Optional[str] = None) -> ScrapeResult:
        """Parses the HTML content to extract NIF validity and postal code."""
        soup = BeautifulSoup(html_content, "html.parser")
        postal_code: Optional[str] = None
        valid_nif: Optional[bool] = None

        # It is known that if the page has a success block, the NIF is valid but the postal code is unknown
        success_block = soup.select_one("div.alert-message.success.block-message")
        if success_block:
            valid_nif = True
            self.logger.warning("[VALIDITY] Valid NIF detected, but could not determine postal code")

        # It is known that if the page has an error block, the NIF is invalid
        error_block = soup.select_one("div.alert-message.error.block-message")
        if error_block:
            valid_nif = False
            self.logger.warning("[VALIDITY] Invalid NIF detected")

        # If no blocks are found, it is unknown if the NIF is valid or not
        else:
            # Look for postal code and description in the detail div
            detail = soup.select_one("div.detail")
            if detail:
                text = detail.get_text(" ", strip=True)
                match = POSTAL_RE.search(text)
                if match:
                    postal_code = match.group(0)
                
                # Extract description from search-title span (always update if found)
                search_title = detail.select_one("span.search-title")
                if search_title:
                    description = search_title.get_text(strip=True)

            if postal_code:
                valid_nif = True

        district = get_district_from_postal(postal_code) if postal_code else None
        
        return self._create_outcome(nif_str, valid_nif, postal_code, district, description)


    def _create_outcome(
        self, 
        nif: str, 
        valid_nif: Optional[bool] = None, 
        postal_code: Optional[str] = None, 
        district: Optional[str] = None,
        description: Optional[str] = None
    ) -> ScrapeResult:
        """Helper to create a consistent ScrapeResult dictionary."""
        return {
            "_id": nif,
            "nif": nif,
            "valid_nif": valid_nif,
            "postal_code": postal_code,
            "district": district,
            "description": description,
        }

    def transform(self, data):
        # Not needed for this step
        pass
