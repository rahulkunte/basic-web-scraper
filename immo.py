import logging
import time
import random
import pandas as pd
import json
from typing import List, Dict, Optional, Tuple
from curl_cffi import requests as cureq
import math

from config import prop_config, headers, cookies


# Initialise the logger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)



class ImmoScraper:
    """
    Scraper for Immoweb, scrapes properties one page at a time, for one postal code, sale status,
    and property type. 
    """

    def __init__(self, delay_range: Tuple[int, int] = (5, 10), max_retries: int = 2):
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.session = None
        self.failed_urls = []
        self.total_scraped = 0
        self.max_pages = 100  # Default max pages, after the first get request we will know the approx # of pages

    def _get_random_delay(self) -> float:
        """Get a random delay with some jitter to avoid detection"""
        base_delay = random.uniform(*self.delay_range)
        # Add up to 20% jitter
        jitter = base_delay * 0.2 * random.uniform(-1, 1)
        return max(1, base_delay + jitter)
    
    def _should_stop_scraping(self, df: pd.DataFrame) -> bool:
        """Check if we should stop scraping (e.g., if we get empty results)"""
        if df is None or df.empty:
            return True
        return False
    
    def scrape_page(self, url: str, page_num: int, prop_type: str) -> Optional[pd.DataFrame]:
        """
        Scrape a single page with enhanced error handling and retries
        
        Args:
            url: URL to scrape
            page_num: Page number for logging
            prop_type: Property type for logging
            
        Returns:
            DataFrame with scraped data or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                response = cureq.get(
                    url, 
                    impersonate="chrome", 
                    headers=headers, 
                    cookies=cookies,
                    timeout=30
                )
                
                if response.status_code == 200:
                    logger.info(f"Success for {prop_type} page {page_num} (attempt {attempt + 1})")
                    
                    # Parse JSON response
                    json_data = json.loads(response.text)
                    results = json_data.get('results', [])
                    total_items = json_data.get('totalItems')

                    if total_items:
                        self.max_pages = math.ceil(total_items / 30)
                    
                    if not results:
                        logger.warning(f"No results found for {prop_type} page {page_num}")
                        return pd.DataFrame()
                    
                    df = pd.json_normalize(results)
                    self.total_scraped += len(df)
                    
                    logger.info(f"Scraped {len(df)} items from {prop_type} page {page_num}")
                    
                    return df
                    
                elif response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 60  # Exponential backoff
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry {attempt + 1}")
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code == 404:
                    logger.info(f"Page not found (404) for {prop_type} page {page_num}. Stopping pagination.")
                    return None  # Signal to stop pagination
                    
                else:
                    logger.warning(f"HTTP {response.status_code} for {prop_type} page {page_num} (attempt {attempt + 1})")
                    
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {prop_type} page {page_num}: {e}")
            except Exception as e:
                logger.error(f"Error scraping {prop_type} page {page_num} (attempt {attempt + 1}): {e}")
            
            # Wait before retry with exponential backoff
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * random.uniform(5, 10)
                time.sleep(wait_time)
        
        # If we get here, all attempts failed
        self.failed_urls.append(url)
        logger.error(f"Failed to scrape {url} after {self.max_retries} attempts")
        return pd.DataFrame()

    def scrape_all_pages(self, base_url: str, prop_type: str, post_code: int, city: str) -> pd.DataFrame:
        """
        Scrape all pages for a property type and postal code
        
        Args:
            base_url: Base URL template
            prop_type: Property type to scrape
            post_code: postal code to scrape
            city: city/municipality to scrape
            
        Returns:
            Combined DataFrame with all results
        """
        all_data = []  
        
        logger.info(f"Starting to scrape {prop_type} in postal code {post_code}")
        
        consecutive_empty = 0
        max_consecutive_empty = 1  # Stop after 1 consecutive empty results arrays
        
        for page in range(1, self.max_pages + 1):
            url = base_url.format(prop_type, city, post_code, page)
            
            logger.info(f"Scraping {prop_type} page {page}/{self.max_pages}")
            
            df = self.scrape_page(url, page, prop_type)
            
            if df is None:  # 404 or similar - stop pagination
                logger.info(f"Stopping pagination for {prop_type} at page {page}")
                break
            
            if df.empty:
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    logger.info(f"Stopping after {consecutive_empty} consecutive empty pages")
                    break
            else:
                consecutive_empty = 0
                all_data.append(df)
            
            # Dynamic delay based on success/failure
            delay = self._get_random_delay()
            if consecutive_empty > 0:
                delay *= 0.5  # Shorter delay for empty pages
            
            time.sleep(delay)
        
        # Combine all DataFrames at once
        final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        
        logger.info(f"Completed scraping {prop_type}: {len(final_df)} total records")
        
        return final_df


def main(postal_codes: pd.DataFrame, types: List[str] = ['sale', 'rent']):
    all_results = []  
    all_failed_urls = []

    for idx, row in postal_codes.iterrows():
        post_code = row['Postal Code']
        city = row['municipality']

        logger.info(f"Starting scraper for postcode: {post_code}, city: {city}")

        location_results = []

        for transaction_type in types:
            config = prop_config[transaction_type]
            base_url = config["url"]
            prop_types = config["prop_types"]
            
            scraper = ImmoScraper(
                delay_range=(2, 5),
                max_retries=2
            )

            logger.info(f"Starting scraper for {transaction_type}")

            for prop in prop_types:
                logger.info(f"Starting scrape for property type: {prop}")
                
                try:
                    df = scraper.scrape_all_pages(
                        base_url=base_url, 
                        prop_type=prop, 
                        post_code=post_code, 
                        city=city
                    )

                    if not df.empty:
                        df['scraped_at'] = pd.Timestamp.now().date()
                        # df['transaction_type'] = transaction_type  
                        df['postal_code'] = post_code
                        df['city'] = city

                        location_results.append(df)
                        
                        logger.info(f"Added {len(df)} records for {transaction_type} - {prop} in postcode {post_code}, city: {city}")
                    
                    time.sleep(random.uniform(10, 20))
                    
                except Exception as e:
                    logger.error(f"Error scraping {transaction_type} - {prop}: {e}")
                    continue
                    
                all_failed_urls.extend(scraper.failed_urls)

            if transaction_type != types[-1]:  
                time.sleep(random.uniform(30, 45))

        # Combine location results
        if location_results:
            combined_location_results = pd.concat(location_results, ignore_index=True)
            all_results.append(combined_location_results)
            
            logger.info(f"Completed {post_code}, {city}: {len(combined_location_results)} total records")

            # Save results for each postcode in a separate file as backup
            logger.info(f"Saving {len(combined_location_results)} results for {post_code}")
            
            filename = f"~/Projects/Scraping/data/immoweb_{post_code}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_location_results.to_csv(filename, index=False)
        else:
            logger.warning(f"No data collected for {post_code}, {city}")

        if idx < len(postal_codes) - 1:  
            location_delay = random.uniform(60, 120)  # 1-2 minutes between locations
            logger.info(f"Break between locations: {location_delay:.1f}s")
            time.sleep(location_delay)

    # Combine all results
    final_results = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
    
    logger.info(f"Scraping completed. Total records: {len(final_results)}")

    # Save results to CSV
    if not final_results.empty:
        filename = f"~/Projects/Scraping/data/immoweb_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_results.to_csv(filename, index=False)
        logger.info(f"Saved {len(final_results)} results to {filename}")


    if all_failed_urls:
        pd.DataFrame(all_failed_urls, columns=['failed_url']).to_csv(f"~/Projects/Scraping/data/failed_urls_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        logger.warning(f"Saved {len(all_failed_urls)} failed URLs to CSV")

    return final_results


if __name__ == "__main__":
    postcodes = pd.read_csv("postcodes.csv")
    results = main(postcodes.sample(2), ['sale', 'rent'])