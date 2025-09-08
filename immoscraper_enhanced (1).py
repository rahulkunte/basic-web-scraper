import logging
import time
import random
import pandas as pd
import json
import math
import asyncio
from typing import List, Dict, Optional, Tuple
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
        self.semaphore = asyncio.Semaphore(max_concurrent)  # limit concurrency

    def _get_random_delay(self) -> float:
        base_delay = random.uniform(*self.delay_range)
        jitter = base_delay * 0.2 * random.uniform(-1, 1)
        return max(0.5, base_delay + jitter)

    async def scrape_page(self, session: cureq.AsyncSession, url: str, page_num: int, prop_type: str) -> Optional[pd.DataFrame]:
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
                        self.max_pages = math.ceil(total_items / 30)

                    if not results:
                        project.log(
                            extra=f"No results found for {prop_type} page {page_num}",
                            subject="Immoweb scraper",
                            level=logging.WARNING,
                        )
                        return pd.DataFrame()

                    df = pd.json_normalize(results)
                    self.total_scraped += len(df)
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
                        extra=f"Page not found (404) for {prop_type} page {page_num}",
                        subject="Immoweb scraper",
                        level=logging.WARNING,
                    )
                    return None

            except Exception as e:
                project.log(
                    extra=f"Error scraping {prop_type} page {page_num} (attempt {attempt+1}): {e}",
                    subject="Immoweb scraper",
                    level=logging.ERROR,
                )

            if attempt < self.max_retries - 1:
                await asyncio.sleep((2 ** attempt) * random.uniform(1, 3))

        self.failed_urls.append(url)
        return pd.DataFrame()

    async def _validate_location(self, session: cureq.AsyncSession, base_url: str, prop_type: str, post_code: int, city: str) -> bool:
        """Validate if the postal code exists by checking the first page"""
        url = base_url.format(prop_type, city, post_code, 1)
        
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
                labellized_search = json_data.get("labellizedSearch", "")
                
                # If "Belgium" is in the search, it means the postal code doesn't exist
                if "Belgium" in labellized_search:
                    project.log(
                        extra=f"Invalid postal code {post_code} ({city}) - redirected to Belgium-wide search",
                        subject="Immoweb scraper",
                        level=logging.WARNING,
                    )
                    return False
                    
            return True
            
        except Exception as e:
            project.log(
                extra=f"Error validating location {post_code} ({city}): {e}",
                subject="Immoweb scraper", 
                level=logging.ERROR,
            )
            return False  # Skip on validation error

    async def scrape_all_pages(self, base_url: str, prop_type: str, post_code: int, city: str) -> pd.DataFrame:
        # Validate location first with separate session
        async with cureq.AsyncSession() as validation_session:
            if not await self._validate_location(validation_session, base_url, prop_type, post_code, city):
                return pd.DataFrame()  # Return empty DataFrame for invalid locations
        
        project.log(
            extra=f"Starting scrape for {prop_type} in {city} ({post_code}), max {self.max_pages} pages",
            subject="Immoweb scraper",
            level=logging.INFO,
        )
        
        all_data = []
        tasks = []

        async with cureq.AsyncSession() as session:
            for page in range(1, self.max_pages + 1):
                url = base_url.format(prop_type, city, post_code, page)
                tasks.append(self.scrape_page(session, url, page, prop_type))

            # gather with concurrency control
            results = await asyncio.gather(*tasks)

        for df in results:
            if df is None:
                break
            if not df.empty:
                all_data.append(df)

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

    for idx, row in postal_codes.iterrows():
        post_code = row["Postal Code"]
        city = row["municipality"]

        project.log(
            extra=f"Processing location {idx+1}/{len(postal_codes)}: {city} ({post_code})",
            subject="Immoweb scraper",
            level=logging.INFO,
        )

        location_results = []
        location_invalid = False  # Flag to track if location is invalid

        for transaction_type in types:
            if location_invalid:  # Skip remaining transaction types if location is invalid
                break
                
            config = prop_pc_config[transaction_type]
            base_url = config["url"]
            prop_types = config["prop_types"]

            scraper = ImmoScraperAsync(delay_range=(0.8, 2.0), max_retries=2, max_concurrent=5)

            for prop in prop_types:
                try:
                    df = await scraper.scrape_all_pages(base_url, prop, post_code, city)
                    
                    if df == "INVALID_LOCATION":
                        project.log(
                            extra=f"Skipping postal code {post_code} ({city}) - location not found",
                            subject="Immoweb scraper",
                            level=logging.WARNING,
                        )
                        location_invalid = True
                        break  # Break out of property types loop
                        
                    if not df.empty:
                        df["scraped_at"] = pd.Timestamp.now().date()
                        df["postal_code"] = post_code
                        df["city"] = city
                        location_results.append(df)

                    await asyncio.sleep(random.uniform(1, 3))  # short pause per prop type

                except Exception as e:
                    project.log(
                        extra=f"Error scraping {transaction_type} - {prop}: {e}",
                        subject="Immoweb scraper",
                        level=logging.ERROR,
                    )

                all_failed_urls.extend(scraper.failed_urls)

            await asyncio.sleep(random.uniform(5, 10))  # between transaction types

        # Skip saving if location was invalid
        if location_invalid:
            continue

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
                extra=f"No data found for {city} ({post_code})",
                subject="Immoweb scraper",
                level=logging.WARNING,
            )

        await asyncio.sleep(random.uniform(5, 15))  # between postcodes

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

    if all_failed_urls:
        failed_filename = f"{path}/external/failed_urls_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(all_failed_urls, columns=["failed_url"]).to_csv(failed_filename, index=False)
        
        project.log(
            extra=f"{len(all_failed_urls)} failed URLs saved to {failed_filename}",
            subject="Immoweb scraper",
            level=logging.WARNING,
        )

    return final_results


if __name__ == "__main__":
    postcodes = pd.read_csv(f"{path}/external/postcodes.csv")
    results = asyncio.run(main(postcodes.loc[44:, :], ["sale", "rent"]))
