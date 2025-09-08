import logging
import time
import random
import pandas as pd
import json
import math
import asyncio
from typing import List, Dict, Optional, Tuple, Union
from curl_cffi import requests as cureq
from dscommons.database import Sybase
from dscommons.vakwerking import Project

from config import headers, cookies, PROJECT_ROOT, path, prop_pc_config

project = Project(PROJECT_ROOT)
sybase = Sybase(project)
sybase2 = Sybase(project=project, insert_strategy="dbisql")


class ImmoScraperAsync:
    def __init__(self, delay_range: Tuple[float, float] = (0.8, 2.0), max_retries: int = 2, max_concurrent: int = 5):
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.failed_urls = []
        self.total_scraped = 0
        self.max_pages = 100
        self.semaphore = asyncio.Semaphore(max_concurrent)

    def _get_random_delay(self) -> float:
        base_delay = random.uniform(*self.delay_range)
        jitter = base_delay * 0.2 * random.uniform(-1, 1)
        return max(0.5, base_delay + jitter)

    async def validate_postal_code(self, session: cureq.AsyncSession, post_code: int, city: str) -> bool:
        """
        Validate if postal code exists by checking any property type.
        Returns True if valid, False if invalid.
        """
        # Use the first available transaction type and property type for validation
        first_transaction = list(prop_pc_config.keys())[0]  # 'sale' or 'rent'
        config = prop_pc_config[first_transaction]
        base_url = config["url"]
        first_prop_type = config["prop_types"][0]  # First property type
        
        validation_url = base_url.format(first_prop_type, city, post_code, 1)
        
        project.log(
            extra=f"Validating postal code {post_code} ({city}) using URL: {validation_url}",
            subject="Immoweb scraper",
            level=logging.INFO,
        )
        
        try:
            async with self.semaphore:
                r = await session.get(
                    validation_url,
                    impersonate="chrome", 
                    headers=headers,
                    cookies=cookies,
                    timeout=30
                )
                
                if r.status_code == 200:
                    json_data = json.loads(r.text)
                    labellized_search = json_data.get("labellizedSearch", "")
                    
                    project.log(
                        extra=f"Validation response for {post_code}: labellizedSearch = '{labellized_search}'",
                        subject="Immoweb scraper",
                        level=logging.DEBUG,
                    )
                    
                    # If "Belgium" is in the search, it means the postal code doesn't exist
                    if "Belgium" in labellized_search:
                        project.log(
                            extra=f"INVALID postal code {post_code} ({city}) - redirected to Belgium-wide search",
                            subject="Immoweb scraper",
                            level=logging.WARNING,
                        )
                        return False
                    else:
                        project.log(
                            extra=f"VALID postal code {post_code} ({city})",
                            subject="Immoweb scraper",
                            level=logging.INFO,
                        )
                        return True
                    
                else:
                    project.log(
                        extra=f"Validation failed for {post_code} ({city}) - HTTP {r.status_code}",
                        subject="Immoweb scraper",
                        level=logging.WARNING,
                    )
                    return False  # Assume invalid if we can't validate
                    
        except Exception as e:
            project.log(
                extra=f"Error validating postal code {post_code} ({city}): {e}",
                subject="Immoweb scraper", 
                level=logging.ERROR,
            )
            return False  # Assume invalid on validation error

    async def scrape_page(self, session: cureq.AsyncSession, url: str, page_num: int, prop_type: str) -> Optional[pd.DataFrame]:
        """Scrape a single page and return DataFrame or None if should stop pagination"""
        for attempt in range(self.max_retries):
            try:
                async with self.semaphore:
                    r = await session.get(
                        url,
                        impersonate="chrome",
                        headers=headers,
                        cookies=cookies,
                        timeout=30
                    )

                if r.status_code == 200:
                    json_data = json.loads(r.text)
                    results = json_data.get("results", [])
                    total_items = json_data.get("totalItems")

                    if total_items:
                        self.max_pages = min(math.ceil(total_items / 30), 50)  # Cap at 50 pages

                    if not results:
                        project.log(
                            extra=f"No results found for {prop_type} page {page_num}",
                            subject="Immoweb scraper",
                            level=logging.DEBUG,
                        )
                        return pd.DataFrame()  # Empty but valid result

                    df = pd.json_normalize(results)
                    self.total_scraped += len(df)
                    
                    project.log(
                        extra=f"Scraped {len(df)} items from {prop_type} page {page_num}",
                        subject="Immoweb scraper",
                        level=logging.DEBUG,
                    )
                    
                    return df

                elif r.status_code == 429:  # rate limited
                    wait_time = (attempt + 1) * 20
                    project.log(
                        extra=f"Rate limited for {prop_type} page {page_num}, waiting {wait_time}s",
                        subject="Immoweb scraper",
                        level=logging.WARNING,
                    )
                    await asyncio.sleep(wait_time)
                    continue

                elif r.status_code == 404:
                    project.log(
                        extra=f"Page not found (404) for {prop_type} page {page_num} - stopping pagination",
                        subject="Immoweb scraper",
                        level=logging.INFO,
                    )
                    return None  # Signal to stop pagination

                else:
                    project.log(
                        extra=f"HTTP {r.status_code} for {prop_type} page {page_num}",
                        subject="Immoweb scraper",
                        level=logging.WARNING,
                    )

            except json.JSONDecodeError as e:
                project.log(
                    extra=f"JSON decode error for {prop_type} page {page_num}: {e}",
                    subject="Immoweb scraper",
                    level=logging.ERROR,
                )
            except Exception as e:
                project.log(
                    extra=f"Error scraping {prop_type} page {page_num} (attempt {attempt+1}): {e}",
                    subject="Immoweb scraper",
                    level=logging.ERROR,
                )

            if attempt < self.max_retries - 1:
                delay = (2 ** attempt) * random.uniform(1, 3)
                await asyncio.sleep(delay)

        # All attempts failed
        self.failed_urls.append(url)
        project.log(
            extra=f"Failed to scrape {url} after {self.max_retries} attempts",
            subject="Immoweb scraper",
            level=logging.ERROR,
        )
        return pd.DataFrame()

    async def scrape_all_pages(self, base_url: str, prop_type: str, post_code: int, city: str) -> pd.DataFrame:
        """Scrape all pages for a specific property type (assuming postal code is already validated)"""
        
        project.log(
            extra=f"Starting scrape for {prop_type} in {city} ({post_code}), max {self.max_pages} pages",
            subject="Immoweb scraper",
            level=logging.INFO,
        )
        
        all_data = []
        consecutive_empty = 0
        max_consecutive_empty = 2

        async with cureq.AsyncSession() as session:
            # Process pages sequentially to handle early stopping logic
            for page in range(1, min(self.max_pages + 1, 21)):  # Limit to 20 pages max
                url = base_url.format(prop_type, city, post_code, page)
                
                df = await self.scrape_page(session, url, page, prop_type)
                
                if df is None:  # 404 or similar - stop pagination
                    project.log(
                        extra=f"Stopping pagination for {prop_type} at page {page}",
                        subject="Immoweb scraper",
                        level=logging.INFO,
                    )
                    break
                
                if df.empty:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        project.log(
                            extra=f"Stopping after {consecutive_empty} consecutive empty pages for {prop_type}",
                            subject="Immoweb scraper",
                            level=logging.INFO,
                        )
                        break
                else:
                    consecutive_empty = 0
                    all_data.append(df)
                
                # Small delay between pages
                await asyncio.sleep(random.uniform(0.5, 1.5))

        total_records = sum(len(df) for df in all_data) if all_data else 0
        project.log(
            extra=f"Completed {prop_type} in {city} ({post_code}): {total_records} records scraped",
            subject="Immoweb scraper",
            level=logging.INFO,
        )

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


async def main(postal_codes: pd.DataFrame, types: List[str] = ["sale", "rent"]):
    project.log(
        extra=f"Starting scrape for {len(postal_codes)} postal codes with types: {', '.join(types)}",
        subject="Immoweb scraper",
        level=logging.INFO,
    )
    
    all_results = []
    all_failed_urls = []
    skipped_postcodes = []

    for idx, row in postal_codes.iterrows():
        post_code = row["Postal Code"]
        city = row["municipality"]

        project.log(
            extra=f"Processing location {idx+1}/{len(postal_codes)}: {city} ({post_code})",
            subject="Immoweb scraper",
            level=logging.INFO,
        )

        # STEP 1: Validate postal code once per location
        scraper = ImmoScraperAsync(delay_range=(0.8, 2.0), max_retries=2, max_concurrent=5)
        
        async with cureq.AsyncSession() as validation_session:
            is_valid = await scraper.validate_postal_code(validation_session, post_code, city)
        
        if not is_valid:
            project.log(
                extra=f"SKIPPING postal code {post_code} ({city}) - invalid location",
                subject="Immoweb scraper",
                level=logging.WARNING,
            )
            skipped_postcodes.append({"postal_code": post_code, "city": city, "reason": "invalid_location"})
            continue  # Skip this entire postal code

        # STEP 2: Scrape all transaction types and property types for this valid postal code
        location_results = []

        for transaction_type in types:
            config = prop_pc_config[transaction_type]
            base_url = config["url"]
            prop_types = config["prop_types"]

            project.log(
                extra=f"Processing {transaction_type} for {city} ({post_code})",
                subject="Immoweb scraper",
                level=logging.INFO,
            )

            for prop_type in prop_types:
                try:
                    project.log(
                        extra=f"Scraping {transaction_type} - {prop_type} for {city} ({post_code})",
                        subject="Immoweb scraper",
                        level=logging.INFO,
                    )
                    
                    df = await scraper.scrape_all_pages(base_url, prop_type, post_code, city)
                        
                    if not df.empty:
                        df["scraped_at"] = pd.Timestamp.now().date()
                        df["postal_code"] = post_code
                        df["city"] = city
                        df["transaction_type"] = transaction_type
                        df["property_type"] = prop_type
                        location_results.append(df)

                        project.log(
                            extra=f"Added {len(df)} records for {transaction_type} - {prop_type}",
                            subject="Immoweb scraper",
                            level=logging.INFO,
                        )
                    else:
                        project.log(
                            extra=f"No results for {transaction_type} - {prop_type}",
                            subject="Immoweb scraper",
                            level=logging.INFO,
                        )

                    await asyncio.sleep(random.uniform(1, 3))  # pause between property types

                except Exception as e:
                    project.log(
                        extra=f"Error scraping {transaction_type} - {prop_type}: {e}",
                        subject="Immoweb scraper",
                        level=logging.ERROR,
                    )

            await asyncio.sleep(random.uniform(5, 10))  # pause between transaction types

        # STEP 3: Save results for this postal code
        if location_results:
            combined_location_results = pd.concat(location_results, ignore_index=True)
            all_results.append(combined_location_results)

            filename = f"{path}/external/immoweb_{post_code}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_location_results.to_csv(filename, index=False)
            
            project.log(
                extra=f"Saved {len(combined_location_results)} records for {city} ({post_code}) to {filename}",
                subject="Immoweb scraper",
                level=logging.INFO,
            )
        else:
            project.log(
                extra=f"No data found for valid postal code {city} ({post_code})",
                subject="Immoweb scraper",
                level=logging.WARNING,
            )

        # Collect failed URLs
        all_failed_urls.extend(scraper.failed_urls)

        # Pause between postal codes
        await asyncio.sleep(random.uniform(10, 20))

    # FINAL RESULTS
    final_results = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

    if not final_results.empty:
        filename = f"{path}/external/immoweb_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_results.to_csv(filename, index=False)
        
        project.log(
            extra=f"Scraping completed: {len(final_results)} total records saved to {filename}",
            subject="Immoweb scraper",
            level=logging.INFO,
        )
    else:
        project.log(
            extra="Scraping completed with no results",
            subject="Immoweb scraper",
            level=logging.WARNING,
        )

    # Save failed URLs
    if all_failed_urls:
        failed_filename = f"{path}/external/failed_urls_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(all_failed_urls, columns=["failed_url"]).to_csv(failed_filename, index=False)
        
        project.log(
            extra=f"{len(all_failed_urls)} failed URLs saved to {failed_filename}",
            subject="Immoweb scraper",
            level=logging.WARNING,
        )

    # Save skipped postal codes
    if skipped_postcodes:
        skipped_filename = f"{path}/external/skipped_postcodes_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(skipped_postcodes).to_csv(skipped_filename, index=False)
        
        project.log(
            extra=f"{len(skipped_postcodes)} invalid postal codes saved to {skipped_filename}",
            subject="Immoweb scraper",
            level=logging.WARNING,
        )

    project.log(
        extra=f"SUMMARY - Processed: {len(postal_codes)}, Valid: {len(postal_codes) - len(skipped_postcodes)}, Skipped: {len(skipped_postcodes)}, Records: {len(final_results)}",
        subject="Immoweb scraper",
        level=logging.INFO,
    )

    return final_results


if __name__ == "__main__":
    postcodes = pd.read_csv(f"{path}/external/postcodes.csv")
    results = asyncio.run(main(postcodes.loc[47:, :], ["sale", "rent"]))  
