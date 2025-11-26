import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import re
import numpy as np
import ast
import uuid
from datetime import datetime
import json
import psycopg2
import os

BASE_URL = "https://www.encuentra24.com"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def get_soup(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return BeautifulSoup(response.content, "html.parser")
        else:
            print(f"Failed to fetch {url}: Status {response.status_code}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None


def scrape_main_listings(page_url, max_pages=1):
    listings = []
    for page in range(1, max_pages + 1):
        print(f"Scraping listing page {page}")
        soup = get_soup(f"{page_url}?page={page}")
        if not soup:
            continue

        # Multiple possible selectors for listing cards
        cards = soup.select("div.d3-ad-tile") or soup.select(".listing-card") or soup.select(".property-card")

        for card in cards:
            # Try multiple selectors for each field
            title_elem = (card.select_one(".d3-ad-tile__title") or
                          card.select_one(".title") or
                          card.select_one("h2") or
                          card.select_one("h3"))

            price_elem = (card.select_one(".d3-ad-tile__price") or
                          card.select_one(".price") or
                          card.select_one(".price-tag"))

            location_elem = (card.select_one(".d3-ad-tile__location span") or
                             card.select_one(".location") or
                             card.select_one(".address"))

            link_elem = (card.select_one("a.d3-ad-tile__description") or
                         card.select_one("a") or
                         card.find("a", href=True))

            if link_elem and link_elem.get('href'):
                full_link = urljoin(BASE_URL, link_elem['href'])
                listings.append({
                    "title": title_elem.get_text(strip=True) if title_elem else None,
                    "price": price_elem.get_text(strip=True) if price_elem else None,
                    "location": location_elem.get_text(strip=True) if location_elem else None,
                    "link": full_link
                })

        time.sleep(2)  # Be respectful with delays
    return listings


def extract_text_safely(element):
    """Safely extract text from an element"""
    if element:
        return element.get_text(" ", strip=True)
    return None


def extract_property_specs(soup):
    """Enhanced property specifications extraction"""
    specs = {
        "area_m2": None,
        "bedrooms": None,
        "bathrooms": None,
        "parking": None,
        "floor": None,
        "raw_specs": []
    }

    # Get all text content
    all_text = soup.get_text()

    # Enhanced pattern matching for common specifications

    # Area in square meters - more comprehensive patterns
    area_patterns = [
        r'(\d+(?:\.\d+)?)\s*m[²2]',
        r'(\d+(?:\.\d+)?)\s*metros?\s*cuadrados?',
        r'área:?\s*(\d+(?:\.\d+)?)\s*m[²2]',
        r'size:?\s*(\d+(?:\.\d+)?)\s*m[²2]',
        r'<strong>área:</strong>\s*(\d+(?:\.\d+)?)\s*m[²2]',
        r'superficie:?\s*(\d+(?:\.\d+)?)\s*m[²2]'
    ]

    for pattern in area_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            specs["area_m2"] = match.group(1)
            specs["raw_specs"].append(f"Area: {match.group(0)}")
            break
        if specs["area_m2"]:
            break

    # Bedrooms/Habitaciones - enhanced patterns
    bedroom_patterns = [
        r'(\d+)\s*habitacion(?:es)?',
        r'(\d+)\s*recámaras?',
        r'(\d+)\s*dormitorios?',
        r'(\d+)\s*bedrooms?',
        r'(\d+)\s*hab\b',
        r'habitaciones?:?\s*(\d+)',
        r'bedrooms?:?\s*(\d+)',
        r'recámaras?:?\s*(\d+)',
        r'<strong>recámaras:</strong>\s*(\d+)',
        r'(\d+)-(\d+)\s*recámaras?'  # Range pattern like "2-3 recámaras"
    ]

    for pattern in bedroom_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            # Handle range patterns (take the first number)
            bedroom_count = match.group(1)
            specs["bedrooms"] = bedroom_count
            specs["raw_specs"].append(f"Bedrooms: {match.group(0)}")
            break
        if specs["bedrooms"]:
            break

    # Bathrooms/Baños - enhanced patterns
    bathroom_patterns = [
        r'(\d+(?:\.\d+)?)\s*baños?',
        r'(\d+(?:\.\d+)?)\s*bathrooms?',
        r'baños?:?\s*(\d+(?:\.\d+)?)',
        r'bathrooms?:?\s*(\d+(?:\.\d+)?)',
        r'<strong>baños:</strong>\s*(\d+(?:\.\d+)?)'
    ]

    for pattern in bathroom_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            specs["bathrooms"] = match.group(1)
            specs["raw_specs"].append(f"Bathrooms: {match.group(0)}")
            break
        if specs["bathrooms"]:
            break

    # Parking - enhanced patterns
    parking_patterns = [
        r'(\d+)\s*estacionamientos?',
        r'(\d+)\s*parking\s*spaces?',
        r'(\d+)\s*garajes?',
        r'estacionamientos?:?\s*(\d+)',
        r'parking:?\s*(\d+)',
        r'estacionamiento:?\s*(\d+)',
        r'<strong>estacionamiento:</strong>\s*(\d+)'
    ]

    for pattern in parking_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            specs["parking"] = match.group(1)
            specs["raw_specs"].append(f"Parking: {match.group(0)}")
            break
        if specs["parking"]:
            break

    # Floor/Piso
    floor_patterns = [
        r'piso\s*(\d+)',
        r'floor\s*(\d+)',
        r'nivel\s*(\d+)',
        r'(\d+)(?:er|do|to|th)?\s*piso',
        r'(\d+)(?:er|do|to|th)?\s*floor'
    ]

    for pattern in floor_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            specs["floor"] = match.group(1)
            specs["raw_specs"].append(f"Floor: {match.group(0)}")
            break
        if specs["floor"]:
            break

    # Look for structured data in specific HTML elements
    spec_elements = soup.find_all(['p', 'div', 'span'],
                                  text=re.compile(r'\d+\s*m[²2]|\d+\s*hab|\d+\s*baño|\d+\s*recámara', re.IGNORECASE))
    for element in spec_elements:
        spec_text = element.get_text(strip=True)
        if len(spec_text) < 100:  # Avoid long descriptions
            specs["raw_specs"].append(spec_text)

    # Look for key-value pairs in the HTML
    key_value_elements = soup.find_all(text=re.compile(r':\s*\d+', re.IGNORECASE))
    for element in key_value_elements:
        text = element.strip()
        if any(keyword in text.lower() for keyword in
               ['área', 'habitacion', 'recámara', 'baño', 'estacionamiento', 'piso']):
            if len(text) < 100:  # Avoid long descriptions
                specs["raw_specs"].append(text)

    # Remove duplicates from raw_specs
    specs["raw_specs"] = list(set(specs["raw_specs"]))

    return specs


def extract_models_enhanced(soup):
    """Enhanced model extraction to handle various structures"""
    models = []

    # Strategy 1: Look for model cards with class patterns
    model_selectors = [
        ".model-card", ".apartment-model", ".unit-type", ".property-model",
        ".model", ".apartment-type", ".floor-plan"
    ]

    for selector in model_selectors:
        model_cards = soup.select(selector)
        for model in model_cards:
            model_data = extract_model_from_element(model)
            if model_data:
                models.append(model_data)

    # Strategy 2: Look for structured data in divs that contain model information
    # Find divs that contain price and area information together
    potential_model_divs = soup.find_all('div')
    for div in potential_model_divs:
        div_text = div.get_text()
        # Check if this div contains both price and area info (likely a model)
        if ('$' in div_text and 'm²' in div_text) or (
                '$' in div_text and any(word in div_text.lower() for word in ['recámara', 'habitacion', 'baño'])):
            model_data = extract_model_from_element(div)
            if model_data and model_data not in models:
                models.append(model_data)

    # Strategy 3: Look for tables with model information
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        current_model = {}

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                for i in range(0, len(cells), 2):
                    if i + 1 < len(cells):
                        key = extract_text_safely(cells[i])
                        value = extract_text_safely(cells[i + 1])
                        if key and value:
                            # Clean and normalize key
                            key_normalized = key.lower().replace(" ", "_").replace(":", "")
                            current_model[key_normalized] = value

        if current_model and len(current_model) > 1:
            models.append(current_model)

    # Strategy 4: Look for lists that might contain model information
    model_lists = soup.find_all(['ul', 'ol'])
    for ul in model_lists:
        list_items = ul.find_all("li")
        if len(list_items) >= 3:  # Only process lists with multiple items
            current_model = {}
            model_name = None

            for i, li in enumerate(list_items):
                text = extract_text_safely(li)
                if not text:
                    continue

                # Check if this might be a model title (first item or contains "modelo")
                if i == 0 or 'modelo' in text.lower():
                    model_name = text
                    current_model['model_title'] = text
                elif ":" in text:
                    # Key-value pair
                    key_val = text.split(":", 1)
                    if len(key_val) == 2:
                        key, val = key_val
                        key_normalized = key.strip().lower().replace(" ", "_")
                        current_model[key_normalized] = val.strip()
                elif '$' in text:
                    current_model['model_price'] = text
                elif 'm²' in text:
                    current_model['area'] = text

            if len(current_model) > 1:  # Only add if we found multiple attributes
                models.append(current_model)

    return models


def extract_model_from_element(element):
    """Extract model data from a single element"""
    model_data = {}

    # Get all text from the element
    element_text = element.get_text()

    # Extract model title
    title_elem = (element.select_one(".model-title") or
                  element.select_one(".title") or
                  element.select_one("h3") or
                  element.select_one("h4") or
                  element.select_one(".model-name"))

    if title_elem:
        model_data["model_title"] = extract_text_safely(title_elem)
    elif "modelo" in element_text.lower():
        # Try to extract model title from text
        modelo_match = re.search(r'modelo\s*\d+[^$]*', element_text, re.IGNORECASE)
        if modelo_match:
            model_data["model_title"] = modelo_match.group(0).strip()

    # Extract price
    price_elem = (element.select_one(".model-price") or
                  element.select_one(".price") or
                  element.select_one(".price-tag"))

    if price_elem:
        model_data["model_price"] = extract_text_safely(price_elem)
    else:
        # Try to extract price from text
        price_match = re.search(r'\$[\\d,]+(?:\.\d+)?', element_text)
        if price_match:
            model_data["model_price"] = price_match.group(0)

    # Extract area
    area_match = re.search(r'(\d+(?:\.\d+)?)\s*m[²2]', element_text)
    if area_match:
        model_data["area"] = area_match.group(0)

    # Extract bedrooms
    bedroom_match = re.search(r'(\d+)\s*(?:recámaras?|habitacion(?:es)?|bedrooms?)', element_text, re.IGNORECASE)
    if bedroom_match:
        model_data["recamaras"] = bedroom_match.group(1)

    # Extract bathrooms
    bathroom_match = re.search(r'(\d+(?:\.\d+)?)\s*baños?', element_text, re.IGNORECASE)
    if bathroom_match:
        model_data["banos"] = bathroom_match.group(1)

    # Extract parking
    parking_match = re.search(r'(\d+)\s*estacionamiento', element_text, re.IGNORECASE)
    if parking_match:
        model_data["estacionamiento"] = parking_match.group(1)

    # Extract other key-value pairs from paragraphs
    paragraphs = element.find_all("p")
    for p in paragraphs:
        text = extract_text_safely(p)
        if text and ":" in text:
            key_val = text.split(":", 1)
            if len(key_val) == 2:
                key, val = key_val
                key_normalized = key.strip().lower().replace(" ", "_")
                model_data[key_normalized] = val.strip()

    # Extract from tags
    strong_tags = element.find_all("strong")
    for strong in strong_tags:
        strong_text = extract_text_safely(strong)
        if strong_text and ":" in strong_text:
            key_val = strong_text.split(":", 1)
            if len(key_val) == 2:
                key, val = key_val
                key_normalized = key.strip().lower().replace(" ", "_")
                # Look for the value in the next sibling or parent
                next_element = strong.next_sibling
                if next_element and hasattr(next_element, 'strip'):
                    val = next_element.strip()
                elif strong.parent:
                    parent_text = strong.parent.get_text()
                    # Extract text that comes after the strong tag
                    strong_index = parent_text.find(strong_text)
                    if strong_index != -1:
                        remaining_text = parent_text[strong_index + len(strong_text):].strip()
                        if remaining_text:
                            val = remaining_text.split('\n')[0].strip()

                if val.strip():
                    model_data[key_normalized] = val.strip()

    return model_data if len(model_data) > 0 else None


def scrape_detail_page(url):
    """Enhanced detail scraper with improved strategies for property pages"""
    soup = get_soup(url)
    if not soup:
        return {}

    print(f"Scraping details from: {url}")

    # Enhanced title extraction
    title = None
    title_selectors = [
        "h1", ".title", ".property-title", ".listing-title", ".ad-title",
        ".header h1", ".main-title", "[class*='title']"
    ]

    for selector in title_selectors:
        elem = soup.select_one(selector)
        if elem:
            title = extract_text_safely(elem)
            if title and len(title) > 5:  # Ensure we got a meaningful title
                break

    # Enhanced subtitle extraction
    subtitle = None
    subtitle_selectors = [
        ".subtitle", ".property-subtitle", ".listing-subtitle", "h2",
        ".location-info", ".address", "[class*='subtitle']"
    ]

    for selector in subtitle_selectors:
        elem = soup.select_one(selector)
        if elem:
            subtitle = extract_text_safely(elem)
            if subtitle and len(subtitle) > 5:
                break

    # Enhanced price extraction
    listing_price = None
    price_selectors = [
        ".price-tag", ".price", ".listing-price", ".property-price",
        "[class*='price']", ".cost", ".valor"
    ]

    for selector in price_selectors:
        elem = soup.select_one(selector)
        if elem:
            price_text = extract_text_safely(elem)
            if price_text and '$' in price_text:
                listing_price = price_text
                break

    # If no price found in elements, search in text
    if not listing_price:
        all_text = soup.get_text()
        price_match = re.search(r'desde\s*\$[\\d,]+(?:\.\d+)?', all_text, re.IGNORECASE)
        if price_match:
            listing_price = price_match.group(0)

    # Enhanced description extraction
    description = None
    desc_keywords = ["descripción", "description", "proyecto", "detalles", "sobre", "acerca"]

    for keyword in desc_keywords:
        # Look for headings with the keyword
        heading = soup.find(['h1', 'h2', 'h3', 'h4'], text=re.compile(keyword, re.IGNORECASE))
        if heading:
            # Get the next content elements
            next_elements = heading.find_next_siblings(['p', 'div'])
            for elem in next_elements:
                elem_text = extract_text_safely(elem)
                if elem_text and len(elem_text) > 20:
                    description = elem_text
                    break
            if description:
                break

        # Look for text containing the keyword
        desc_elem = soup.find(text=re.compile(keyword, re.IGNORECASE))
        if desc_elem:
            parent = desc_elem.find_parent()
            if parent:
                # Look for content in the same card/section
                content_elem = parent.find(['p', 'div'])
                if content_elem:
                    desc_text = extract_text_safely(content_elem)
                    if desc_text and len(desc_text) > 20:
                        description = desc_text
                        break

    models = extract_models_enhanced(soup)
    amenities = []
    amenity_keywords = ["amenidades", "amenities", "servicios", "instalaciones", "comodidades", "facilidades"]

    for keyword in amenity_keywords:
        # Look for sections with amenity keywords
        matching_elements = soup.find_all(text=re.compile(keyword, re.IGNORECASE))
        for element in matching_elements:
            parent = element.find_parent()
            if parent:
                # Look for grid patterns (common for amenities)
                amenity_divs = parent.find_all('div', class_=re.compile('amenity|grid'))
                for div in amenity_divs:
                    amenity_text = extract_text_safely(div)
                    if amenity_text and len(amenity_text.strip()) > 2 and len(amenity_text) < 50:
                        amenities.append(amenity_text.strip())

                # Look for lists
                nearby_lists = parent.find_all(["ul", "ol"])
                for ul in nearby_lists:
                    for li in ul.find_all("li"):
                        amenity_text = extract_text_safely(li)
                        if amenity_text and len(amenity_text.strip()) > 2:
                            amenities.append(amenity_text.strip())

    # Look for divs with amenity-like classes
    amenity_class_patterns = ['amenity', 'feature', 'benefit', 'service']
    for pattern in amenity_class_patterns:
        elements = soup.find_all('div', class_=re.compile(pattern, re.IGNORECASE))
        for elem in elements:
            amenity_text = extract_text_safely(elem)
            if amenity_text and 2 < len(amenity_text) < 50:
                amenities.append(amenity_text.strip())

    # Enhanced apartment features extraction
    apartment_features = []
    feature_keywords = ["características", "features", "apartamento", "incluye", "cuenta con", "dispone"]

    for keyword in feature_keywords:
        matching_elements = soup.find_all(text=re.compile(keyword, re.IGNORECASE))
        for element in matching_elements:
            parent = element.find_parent()
            if parent:
                # Look for benefit-list class or similar patterns
                benefit_elements = parent.find_all('div', class_=re.compile('benefit|feature'))
                for div in benefit_elements:
                    feature_text = extract_text_safely(div)
                    if feature_text and len(feature_text.strip()) > 3:
                        apartment_features.append(feature_text.strip())

                # Look for lists
                nearby_lists = parent.find_all(["ul", "ol"])
                for ul in nearby_lists:
                    for li in ul.find_all("li"):
                        feature_text = extract_text_safely(li)
                        if feature_text and len(feature_text.strip()) > 3:
                            apartment_features.append(feature_text.strip())

    benefit_elements = soup.find_all('div', class_=re.compile('benefit'))
    for elem in benefit_elements:
        benefit_text = extract_text_safely(elem)
        if benefit_text and len(benefit_text.strip()) > 3:
            apartment_features.append(benefit_text.strip())

    # Enhanced additional benefits extraction
    additional_benefits = []
    benefits_keywords = ["beneficios", "benefits", "adicionales", "ventajas", "plus"]

    for keyword in benefits_keywords:
        benefits_sections = soup.find_all(text=re.compile(keyword, re.IGNORECASE))
        for section in benefits_sections:
            parent = section.find_parent()
            if parent:
                benefit_elems = parent.find_all(["li", "div", "p"])
                for elem in benefit_elems:
                    benefit_text = extract_text_safely(elem)
                    if benefit_text and 3 < len(benefit_text) < 100:
                        additional_benefits.append(benefit_text)

    property_specs = extract_property_specs(soup)
    amenities = list(set([a for a in amenities if a]))
    apartment_features = list(set([f for f in apartment_features if f]))
    additional_benefits = list(set([b for b in additional_benefits if b]))

    result = {
        "page_title": title,
        "subtitle": subtitle,
        "listing_price": listing_price,
        "description": description,
        "models": models,
        "amenities": amenities,
        "apartment_features": apartment_features,
        "additional_benefits": additional_benefits,
        "area_m2": property_specs.get("area_m2"),
        "bedrooms": property_specs.get("bedrooms"),
        "bathrooms": property_specs.get("bathrooms"),
        "parking": property_specs.get("parking"),
        "floor": property_specs.get("floor"),
        "property_specs_raw": property_specs.get("raw_specs", [])
    }

    # DEBUG
    print(f"  ✓ Title: {bool(title)} | Price: {bool(listing_price)} | Description: {bool(description)}")
    print(f"  ✓ Models: {len(models)} | Amenities: {len(amenities)} | Features: {len(apartment_features)}")
    print(
        f"  ✓ Benefits: {len(additional_benefits)} | Specs: Area={property_specs.get('area_m2')}, Beds={property_specs.get('bedrooms')}, Baths={property_specs.get('bathrooms')}")

    if models:
        print(f"  ✓ Sample models: {[m.get('model_title', 'Unnamed') for m in models[:2]]}")
    if amenities:
        print(f"  ✓ Sample amenities: {amenities[:3]}")
    if apartment_features:
        print(f"  ✓ Sample features: {apartment_features[:3]}")

    return result


def flatten_models(models):
    """Enhanced model flattening with more comprehensive data"""
    if not models:
        return None

    flattened = []
    for m in models:
        # Start with model title and price
        model_parts = []

        if m.get('model_title'):
            model_parts.append(m['model_title'])

        if m.get('model_price'):
            model_parts.append(f"@ {m['model_price']}")

        # Add area information
        area = m.get('área') or m.get('area') or m.get('size')
        if area:
            model_parts.append(f"Area: {area}")

        # Add bedrooms and bathrooms
        bedrooms = m.get('recámaras') or m.get('recamaras') or m.get('habitaciones') or m.get('bedrooms')
        bathrooms = m.get('baños') or m.get('banos') or m.get('bathrooms')

        if bedrooms or bathrooms:
            room_info = f"{bedrooms or 'N/A'} hab, {bathrooms or 'N/A'} baños"
            model_parts.append(room_info)

        # Add parking if available
        parking = m.get('estacionamiento') or m.get('parking')
        if parking:
            model_parts.append(f"Parking: {parking}")

        # Add any additional relevant information
        for key, value in m.items():
            if key not in ['model_title', 'model_price', 'área', 'area', 'size', 'recámaras', 'recamaras',
                           'habitaciones', 'bedrooms', 'baños', 'banos', 'bathrooms', 'estacionamiento', 'parking']:
                if value and str(value).strip():
                    model_parts.append(f"{key.replace('_', ' ').title()}: {value}")

        flattened.append(" - ".join(model_parts))

    return "; ".join(flattened)


def clean_price(price_str):
    if pd.isna(price_str):
        return np.nan
    # Quita símbolos, separa rangos y toma mínimo
    clean = price_str.replace("B/.", "").replace("$", "").replace(",", "")
    parts = clean.split('-')
    try:
        return float(parts[0])
    except:
        return np.nan


def parse_int(val):
    try:
        return int(val)
    except:
        return np.nan


def parse_list(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []

    if isinstance(val, list):
        # Chequear si todos los elementos son strings (hashables)
        if all(isinstance(item, str) for item in val):
            return list(set(val))  # quitar duplicados
        return val  # lista de dicts o mixto, devolver tal cual

    if isinstance(val, str):
        val = val.strip()
        if val == "":
            return []
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                # Lista parseada, procesar como lista original
                return parse_list(parsed)
        except:
            pass
        # Si no se pudo parsear, asumimos lista separada por comas
        return list({item.strip() for item in val.split(',') if item.strip()})

    return []


def extract_numeric_from_specs(row, field, key):
    """Extrae número desde property_specs_raw si no hay otro."""
    if pd.notna(row[field]):
        return row[field]
    raw = row.get('property_specs_raw', '')
    # Busca patrón "Bedrooms: X habitaciones"
    prefix = f"{key}: "
    for part in raw:
        if prefix in part:
            try:
                return float(part.split(prefix)[1].split()[0])
            except:
                continue
    return np.nan


# --- NEW CLEANING FUNCTION ---
def clean_data(df_raw):
    # 1. Rename 'link' to 'url'
    df_raw.rename(columns={'link': 'url'}, inplace=True)

    # 2. Generate 'id'
    max_bigint = 9223372036854775807
    df_raw['id'] = [abs(hash(uuid.uuid4())) % max_bigint for _ in range(len(df_raw))]

    # 3. Add 'scraped_at'
    df_raw['scraped_at'] = datetime.now().isoformat()

    # 4. Handle 'marketplace_id'
    df_raw['marketplace_id'] = 1

    # 5. Handle 'image_url'
    df_raw['image_url'] = ''

    # 6. Handle 'attributes'
    def create_attributes_json(row):
        attributes = {}
        # Handle list-like columns that are already parsed by parse_list
        if row['amenities']:  # Check if list is not empty
            attributes['amenities'] = row['amenities']
        if row['apartment_features']:  # Check if list is not empty
            attributes['apartment_features'] = row['apartment_features']
        if row['additional_benefits']:  # Check if list is not empty
            attributes['additional_benefits'] = row['additional_benefits']
        if row['models']:  # Check if list is not empty
            attributes['models'] = row['models']

        # Handle other columns, ensuring they are not NaN and converted to string if needed
        if row['property_specs_raw'] is not None:
            attributes['property_specs_raw'] = str(row['property_specs_raw'])
        if row['models_flat'] is not None:
            attributes['models_flat'] = str(row['models_flat'])
        if row['page_title'] is not None:
            attributes['page_title'] = str(row['page_title'])
        if row['subtitle'] is not None:
            attributes['subtitle'] = str(row['subtitle'])
        if row['listing_price'] is not None:
            attributes['listing_price'] = str(row['listing_price'])

        return json.dumps(attributes)

    df_raw['attributes'] = df_raw.apply(create_attributes_json, axis=1)

    # 7. Data Type Conversion and Cleaning
    df_raw['price'] = pd.to_numeric(df_raw['price'], errors='coerce').fillna(0).astype(float)
    df_raw['bathrooms'] = pd.to_numeric(df_raw['bathrooms'], errors='coerce').fillna(0).astype(float)
    df_raw['bedrooms'] = pd.to_numeric(df_raw['bedrooms'], errors='coerce').fillna(0).astype(int)
    df_raw['floor'] = pd.to_numeric(df_raw['floor'], errors='coerce').fillna(0).astype(int)
    df_raw['parking'] = pd.to_numeric(df_raw['parking'], errors='coerce').fillna(0).astype(int)
    df_raw['area_m2'] = pd.to_numeric(df_raw['area_m2'], errors='coerce').fillna(0).astype(float)
    df_raw['description'] = df_raw['description'].fillna('').astype(str)

    # 8. Truncate string columns
    df_raw['title'] = df_raw['title'].astype(str).str[:200]
    df_raw['url'] = df_raw['url'].astype(str).str[:200]
    df_raw['image_url'] = df_raw['image_url'].astype(str).str[:200]
    df_raw['location'] = df_raw['location'].astype(str).str[:255]

    # 9. Select and Reorder Columns
    final_df = df_raw[[
        'price', 'bathrooms', 'bedrooms', 'floor', 'parking', 'id',
        'attributes', 'scraped_at', 'marketplace_id', 'area_m2',
        'title', 'description', 'url', 'image_url', 'location'
    ]]
    return final_df


# --- NEW DATABASE LOADING FUNCTION ---
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST",
                    "localhost")  # Assuming local execution, change to "db" if running inside docker-compose network
DB_PORT = os.getenv("DB_PORT", "5433")  # Exposed port on host

TABLE_NAME = "public.frontend_product"


def create_table_if_not_exists(cur):
    """Creates the listings table if it doesn't already exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        price NUMERIC,
        bathrooms NUMERIC,
        bedrooms INTEGER,
        floor INTEGER,
        parking INTEGER,
        id BIGINT PRIMARY KEY,
        attributes JSONB,
        scraped_at TIMESTAMP WITH TIME ZONE,
        marketplace_id BIGINT,
        area_m2 NUMERIC,
        title VARCHAR(200),
        description TEXT,
        url VARCHAR(200),
        image_url VARCHAR(200),
        location VARCHAR(255)
    );
    """
    cur.execute(create_table_query)
    print(f"Table '{TABLE_NAME}' ensured to exist.")


def load_data_to_db(df_cleaned):
    """Loads data from the cleaned DataFrame into the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        create_table_if_not_exists(cur)

        # Prepare for insertion
        # Convert NaN to None for database compatibility
        df_cleaned = df_cleaned.where(pd.notna(df_cleaned), None)

        for index, row in df_cleaned.iterrows():
            insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                price, bathrooms, bedrooms, floor, parking, id,
                attributes, scraped_at, marketplace_id, area_m2,
                title, description, url, image_url, location
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            ) ON CONFLICT (id) DO NOTHING;
            """
            attributes_data = row['attributes'] if row['attributes'] else None

            # Explicitly handle description to ensure it's not None
            description_data = row['description'] if row['description'] is not None else ""

            cur.execute(insert_query, (
                row['price'], row['bathrooms'], row['bedrooms'], row['floor'], row['parking'], row['id'],
                attributes_data, row['scraped_at'], row['marketplace_id'], row['area_m2'],
                row['title'], description_data, row['url'], row['image_url'], row['location']
            ))

        conn.commit()
        print(f"Data loaded successfully into '{TABLE_NAME}'.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")


