from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import scraper
import time
import pandas as pd

class ScrapeRequest(BaseModel):
    pages: int

app = FastAPI()

def run_scraping_task(pages: int):
    """
    A function that runs the scraping and processing logic.
    """
    base_list_url = "https://www.encuentra24.com/panama-es/bienes-raices"

    print(f"🔍 Starting to scrape {pages} pages of listings...")
    listings = scraper.scrape_main_listings(base_list_url, max_pages=pages)
    print(f"Found {len(listings)} listings")

    if not listings:
        print("No listings found. Check the main listing scraper selectors.")
        return

    all_data = []

    for i, listing in enumerate(listings):
        print(f"\n[{i + 1}/{len(listings)}] Processing: {listing['title'][:50]}...")
        detail = scraper.scrape_detail_page(listing["link"])

        # Combine listing and detail data
        row = {**listing, **detail}
        row["models_flat"] = scraper.flatten_models(row.get("models"))
        all_data.append(row)

        time.sleep(2)

    # Create DataFrame from scraped data
    df_raw = pd.DataFrame(all_data)

    # Apply initial cleaning from original main.py
    df_raw['title'] = df_raw['title'].fillna('').str.strip()
    df_raw['link'] = df_raw['link'].fillna('').str.strip()
    df_raw = df_raw[df_raw['link'].str.startswith('http')]

    df_raw['price'] = df_raw['price'].apply(scraper.clean_price)
    df_raw['area_m2'] = pd.to_numeric(df_raw['area_m2'], errors='coerce')

    df_raw['bedrooms'] = df_raw['bedrooms'].apply(scraper.parse_int)
    df_raw['bathrooms'] = df_raw['bathrooms'].apply(scraper.parse_int)
    df_raw['parking'] = df_raw['parking'].apply(scraper.parse_int)

    if 'property_specs_raw' in df_raw.columns:
        df_raw['bedrooms'] = df_raw.apply(lambda r: scraper.extract_numeric_from_specs(r, 'bedrooms', 'Bedrooms'),
                                          axis=1).fillna(
            df_raw['bedrooms'])
        df_raw['bathrooms'] = df_raw.apply(lambda r: scraper.extract_numeric_from_specs(r, 'bathrooms', 'Bathrooms'),
                                           axis=1).fillna(
            df_raw['bathrooms'])
        df_raw['parking'] = df_raw.apply(lambda r: scraper.extract_numeric_from_specs(r, 'parking', 'Parking'), axis=1).fillna(
            df_raw['parking'])

    for col in ['amenities', 'apartment_features', 'additional_benefits', 'models_flat', 'models']:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].apply(scraper.parse_list)

    df_raw['description'] = df_raw['description'].fillna('').str.strip()
    df_raw['subtitle'] = df_raw['subtitle'].fillna('').str.strip()
    df_raw['page_title'] = df_raw['page_title'].fillna('').str.strip()

    # Call the new cleaning function
    df_cleaned = scraper.clean_data(df_raw.copy())
    df_cleaned.to_csv("encuentra24_final_cleaned.csv", index=False)

    # Load data to database
    scraper.load_data_to_db(df_cleaned)

    print(f"\n✅ Scraping and data loading complete! Loaded {len(df_cleaned)} records to '{scraper.TABLE_NAME}'")


@app.post("/scrape")
async def scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Starts a background task to scrape Encuentra24 listings.
    """
    background_tasks.add_task(run_scraping_task, request.pages)
    return {"message": f"Scraping for {request.pages} pages initiated in the background."}
