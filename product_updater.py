"""
Product Updater Script
======================
Updates scraped products in the database by:
1. Fetching latest data from Shopify JSON endpoint (brand_url + .js)
2. Updating product fields (title, description, prices, variants, options, images, availability)
3. Scraping size charts for apparel products and storing as HTML

Features:
- Progress tracking in progress.json (resume from where it stopped)
- Error logging in error.json with product URLs
- Console logging for progress
- Batch processing for 50k+ products
"""

import asyncio
import json
import os
import sys
import time
import httpx
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv

# Import the size chart scraper
# Import the size chart scraper
from scraper import SizeChartScraper

# Import generic product scraper for non-Shopify fallback
try:
    from generic_product_scraper import fetch_generic_product_data
except ImportError:
    print("Warning: generic_product_scraper module not found. Non-Shopify fallback disabled.")
    fetch_generic_product_data = None

def adapt_generic_data_to_shopify_format(generic_data):
    """
    Converts generic scraper data to Shopify JSON structure
    so existing update logic works without changes.
    """
    if not generic_data:
        return None
        
    price = generic_data.get("price", 0)
    original_price = generic_data.get("original_price", 0)
    
    # Construct minimal Shopify-compatible structure
    return {
        "product": {
            "title": generic_data.get("title"),
            "body_html": generic_data.get("description"),
            "vendor": "Generic", 
            "product_type": "Apparel",
            "variants": [
                {
                    "price": str(price),
                    "compare_at_price": str(original_price) if original_price > price else None,
                    "sku": "",
                    "option1": "Default Title", 
                    "availability": True,
                    "inventory_quantity": 10
                }
            ],
            "images": [{"src": img} for img in generic_data.get("images", [])],
            "tags": "",
            "options": [{"name": "Title", "values": ["Default Title"]}]
        }
    }

# Load environment variables
load_dotenv()

# File paths for tracking
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(SCRIPT_DIR, "progress.json")
ERROR_FILE = os.path.join(SCRIPT_DIR, "error.json")

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "surgeDb"),
    "user": os.getenv("DB_USER", "pgadmin"),
    "password": os.getenv("DB_PASSWORD", ""),
}


def load_progress():
    """Load progress from progress.json"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Could not load progress file: {e}")
    return {
        "last_processed_id": None,
        "total_processed": 0,
        "total_updated": 0,
        "total_errors": 0,
        "started_at": None,
        "last_updated_at": None
    }


def save_progress(progress):
    """Save progress to progress.json"""
    progress["last_updated_at"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def load_errors():
    """Load existing errors from error.json"""
    if os.path.exists(ERROR_FILE):
        try:
            with open(ERROR_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Could not load error file: {e}")
    return {"errors": []}


def save_error(product_id, brand_url, error_message, error_type="fetch"):
    """Append error to error.json"""
    errors = load_errors()
    errors["errors"].append({
        "product_id": product_id,
        "brand_url": brand_url,
        "error": str(error_message),
        "type": error_type,
        "timestamp": datetime.now().isoformat()
    })
    with open(ERROR_FILE, "w") as f:
        json.dump(errors, f, indent=2)


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def fetch_shopify_json(brand_url: str, timeout: int = 30) -> dict:
    """
    Fetch product data from Shopify JSON endpoint.
    Appends .js to the brand_url to get JSON response.
    """
    # Construct JSON URL
    json_url = brand_url.rstrip("/") + ".js"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }
    
    with httpx.Client(timeout=timeout, follow_redirects=True, verify=False) as client:
        response = client.get(json_url, headers=headers)
        response.raise_for_status()
        return response.json()


def convert_price(price_in_paisa) -> float:
    """Convert price from paisa (integer) to rupees (decimal)"""
    if price_in_paisa is None:
        return None
    return float(price_in_paisa) / 100.0


def table_to_html(table_data: list) -> str:
    """Convert table data (list of rows) to HTML table"""
    if not table_data or len(table_data) == 0:
        return ""
    
    html = '<table class="size-chart-table">\n'
    
    # First row as header
    html += "  <thead>\n    <tr>\n"
    for cell in table_data[0]:
        html += f"      <th>{cell}</th>\n"
    html += "    </tr>\n  </thead>\n"
    
    # Rest as body
    if len(table_data) > 1:
        html += "  <tbody>\n"
        for row in table_data[1:]:
            html += "    <tr>\n"
            for cell in row:
                html += f"      <td>{cell}</td>\n"
            html += "    </tr>\n"
        html += "  </tbody>\n"
    
    html += "</table>"
    return html


def images_to_html(images: list) -> str:
    """Convert image URLs to HTML img tags"""
    if not images:
        return ""
    
    html = '<div class="size-chart-images">\n'
    for img_url in images:
        # Ensure URL has https protocol
        if img_url.startswith("//"):
            img_url = "https:" + img_url
        html += f'  <img src="{img_url}" alt="Size Chart" loading="lazy" />\n'
    html += '</div>'
    return html


def result_to_html(result: dict) -> str:
    """Convert scraper result to full HTML"""
    parts = []
    
    if result.get("table"):
        parts.append(table_to_html(result["table"]))
    
    if result.get("images"):
        parts.append(images_to_html(result["images"]))
    
    # Handle text-based size info (HTML content from containers like dopamean.in)
    if result.get("textHtml"):
        # Wrap the extracted HTML in a size-info div
        text_html = result["textHtml"]
        parts.append(f'<div class="size-chart-text">\n{text_html}\n</div>')
    
    if not parts:
        return None
    
    return '<div class="size-chart">\n' + "\n".join(parts) + '\n</div>'


async def scrape_size_chart(brand_url: str) -> str:
    """
    Scrape size chart from product page and return as HTML.
    Returns None if no size chart found.
    """
    try:
        scraper = SizeChartScraper(brand_url, headless=True)
        
        # Monkey-patch the run method to return result instead of printing
        async with __import__('playwright.async_api', fromlist=['async_playwright']).async_playwright() as p:
            args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certificate-errors",
                "--ignore-certificate-errors-spki-list",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ]
            
            context = await p.chromium.launch_persistent_context(
                scraper.user_data_dir,
                headless=True,
                args=args,
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            
            page = context.pages[0] if context.pages else await context.new_page()
            
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            try:
                await page.goto(brand_url, wait_until="domcontentloaded", timeout=60000)
            except Exception:
                pass
            
            await page.wait_for_timeout(2000)
            
            # Detect type
            if scraper.domain in scraper.brand_cache:
                scraper.detected_type = scraper.brand_cache[scraper.domain]['type']
            else:
                await scraper.detect_size_chart_type(page)
            
            # Interact with triggers
            await scraper.interact_with_triggers(page)
            await page.wait_for_timeout(2000)
            
            # Extract content
            result = await scraper.extract_content(page)
            
            # NO FALLBACK - only use DOM extraction results
            # The Shopify fallback was returning random product images as size charts
            
            await context.close()
            
            return result_to_html(result)
            
    except Exception as e:
        print(f"    [SIZE CHART ERROR] {e}")
        return None


def update_product(conn, product_id: int, shopify_data: dict, size_chart_html: str = None):
    """Update product in database with new data"""
    
    # Map fields
    title = shopify_data.get("title")
    description = shopify_data.get("description")
    price_discounted = convert_price(shopify_data.get("price"))
    price_original = convert_price(shopify_data.get("compare_at_price"))
    is_available = shopify_data.get("available", False)
    variants = shopify_data.get("variants", [])
    options = shopify_data.get("options", [])
    images = shopify_data.get("images", [])
    
    # Clean up images - ensure https protocol
    cleaned_images = []
    for img in images:
        if isinstance(img, str):
            if img.startswith("//"):
                img = "https:" + img
            cleaned_images.append(img)
    
    # Build update query
    update_fields = []
    params = []
    
    if title:
        update_fields.append("title = %s")
        params.append(title)
    
    if description:
        update_fields.append("description = %s")
        params.append(description)
    
    if price_discounted is not None:
        update_fields.append("price_discounted = %s")
        params.append(price_discounted)
    
    if price_original is not None:
        update_fields.append("price_original = %s")
        params.append(price_original)
    
    update_fields.append("is_available = %s")
    params.append(is_available)
    
    if variants:
        update_fields.append("variants = %s")
        params.append(Json(variants))
    
    if options:
        update_fields.append("options = %s")
        params.append(Json(options))
    
    if cleaned_images:
        # Use PostgreSQL array format, not JSON
        update_fields.append("images = %s")
        params.append(cleaned_images)
    
    if size_chart_html:
        update_fields.append("size_chart = %s")
        params.append(size_chart_html)
    elif size_chart_html == "":
        # Explicitly clear size_chart for apparel products with no chart found
        update_fields.append("size_chart = NULL")
    
    # Add product_id for WHERE clause
    params.append(product_id)
    
    query = f"""
        UPDATE scraped_products
        SET {', '.join(update_fields)}
        WHERE id = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def get_products_to_update(conn, last_id: int = None, batch_size: int = 100):
    """Fetch batch of products to update, ordered by id"""
    query = """
        SELECT id, brand_url, category, title
        FROM scraped_products
        WHERE brand_url IS NOT NULL AND brand_url != ''
    """
    params = []
    
    if last_id is not None:
        query += " AND id > %s"
        params.append(last_id)
    
    query += " ORDER BY id ASC LIMIT %s"
    params.append(batch_size)
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def get_total_count(conn, last_id: int = None):
    """Get total count of products remaining"""
    query = """
        SELECT COUNT(*) as count
        FROM scraped_products
        WHERE brand_url IS NOT NULL AND brand_url != ''
    """
    params = []
    
    if last_id is not None:
        query += " AND id > %s"
        params.append(last_id)
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()["count"]


async def main():
    """Main function to run the product updater"""
    print("=" * 60)
    print("PRODUCT UPDATER - Starting")
    print("=" * 60)
    
    # Load progress
    progress = load_progress()
    
    if progress["started_at"] is None:
        progress["started_at"] = datetime.now().isoformat()
    
    last_id = progress.get("last_processed_id")
    
    if last_id:
        print(f"[RESUME] Continuing from product ID: {last_id}")
        print(f"  - Previously processed: {progress['total_processed']}")
        print(f"  - Previously updated: {progress['total_updated']}")
        print(f"  - Previous errors: {progress['total_errors']}")
    else:
        print("[START] Fresh start - no previous progress found")
    
    # Connect to database
    print("\n[DB] Connecting to database...")
    try:
        conn = get_db_connection()
        print("[DB] Connected successfully!")
    except Exception as e:
        print(f"[DB ERROR] Failed to connect: {e}")
        return
    
    # Get total remaining count
    remaining = get_total_count(conn, last_id)
    print(f"[INFO] Products remaining: {remaining:,}")
    
    batch_size = 50
    processed = 0
    updated = 0
    errors = 0
    
    try:
        while True:
            # Fetch batch
            products = get_products_to_update(conn, last_id, batch_size)
            
            if not products:
                print("\n[DONE] No more products to process!")
                break
            
            print(f"\n[BATCH] Processing {len(products)} products...")
            
            for product in products:
                product_id = product["id"]
                brand_url = product["brand_url"]
                category = (product.get("category") or "").lower().strip()
                title = product.get("title") or "Unknown"
                
                processed += 1
                progress["total_processed"] += 1
                
                # Log progress
                total_done = progress["total_processed"]
                print(f"\n[{total_done:,}/{total_done + remaining - processed:,}] ID: {product_id}")
                print(f"  Title: {title[:50]}...")
                print(f"  URL: {brand_url[:60]}...")
                print(f"  Category: {category or 'N/A'}")
                
                # Initialize fallback result
                shopify_data = None
                
                try:
                    # Inner try to handle fallback specific logic
                    try:
                        # Fetch Shopify JSON
                        print("  [FETCH] Getting Shopify JSON...")
                        shopify_data = fetch_shopify_json(brand_url)
                        print(f"  [FETCH] Success - Title: {shopify_data.get('title', 'N/A')[:40]}...")
                    except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError, Exception) as e_shopify:
                        # If generic scraper is available, try fallback
                        if fetch_generic_product_data:
                            print(f"  [FETCH] Shopify JSON failed: {str(e_shopify)[:100]}")
                            print("  [FALLBACK] Attempting Generic Scraper...")
                            try:
                                # Use await since Generic Scraper is async
                                generic_data = await fetch_generic_product_data(brand_url)
                                if generic_data and generic_data.get('title'):
                                    shopify_data = adapt_generic_data_to_shopify_format(generic_data)
                                    print(f"  [FALLBACK] Success - Title: {shopify_data['product']['title'][:40]}...")
                                else:
                                    print("  [FALLBACK] Failed or returned empty data.")
                                    # Re-raise original error if fallback also failed
                                    raise e_shopify
                            except Exception as e_generic:
                                print(f"  [FALLBACK] Generic Scraper failed: {e_generic}")
                                raise e_shopify # Raise original error for logging
                        else:
                            raise e_shopify
                    
                    # Scrape size chart (apparel only)
                    size_chart_html = None
                    if category == "apparel":
                        # Check if we should scrape size chart
                        # If we used generic scraper, size chart logic is separate and robust
                        print("  [SIZE CHART] Scraping size chart (apparel)...")
                        # scrape_size_chart is async
                        size_chart_html = await scrape_size_chart(brand_url)
                        if size_chart_html:
                            print(f"  [SIZE CHART] Found! HTML length: {len(size_chart_html)} chars")
                        else:
                            size_chart_html = ""
                            print("  [SIZE CHART] Not found - will clear field")
                    else:
                        print("  [SIZE CHART] Skipped (not apparel)")
                        
                    # Update database
                    print("  [UPDATE] Updating database...")
                    update_product(conn, product_id, shopify_data, size_chart_html)
                    print("  [UPDATE] Success!")
                    
                    updated += 1
                    progress["total_updated"] += 1
                    
                except httpx.HTTPStatusError as e:
                    print(f"  [ERROR] HTTP {e.response.status_code}: {e}")
                    try:
                        conn.rollback()
                    except:
                        pass
                    
                    # If 404, DELETE the product
                    if e.response.status_code == 404:
                        print(f"  [DELETE] verified 404 for {brand_url}")
                        try:
                            with conn.cursor() as cur:
                                cur.execute("DELETE FROM scraped_products WHERE id = %s", (product_id,))
                            conn.commit()
                            print(f"  [DELETE] Successfully removed product ID {product_id}")
                            processed += 1 # Count as processed
                            progress["total_processed"] += 1
                            continue # Skip to next product
                        except Exception as del_e:
                            print(f"  [DELETE ERROR] Failed to delete: {del_e}")
                            conn.rollback()

                    save_error(product_id, brand_url, str(e), "http")
                    errors += 1
                    progress["total_errors"] += 1
                    
                except httpx.RequestError as e:
                    print(f"  [ERROR] Request failed: {e}")
                    try:
                        conn.rollback()
                    except:
                        pass
                    save_error(product_id, brand_url, str(e), "request")
                    errors += 1
                    progress["total_errors"] += 1
                    
                except Exception as e:
                    print(f"  [ERROR] Unexpected: {e}")
                    # Rollback transaction to prevent 'transaction aborted' errors
                    try:
                        conn.rollback()
                    except:
                        pass
                    save_error(product_id, brand_url, str(e), "unknown")
                    errors += 1
                    progress["total_errors"] += 1
                
                # Update last processed ID
                last_id = product_id
                progress["last_processed_id"] = last_id
                
                # Save progress every 10 products
                if processed % 10 == 0:
                    save_progress(progress)
                    print(f"\n  [PROGRESS SAVED] {progress['total_processed']:,} processed, {progress['total_updated']:,} updated, {progress['total_errors']:,} errors")
            
            # Save progress after each batch
            save_progress(progress)
            
            # Small delay between batches to avoid overwhelming
            await asyncio.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Saving progress...")
        save_progress(progress)
        print("[SAVED] You can resume later!")
        
    finally:
        conn.close()
        save_progress(progress)
        
        print("\n" + "=" * 60)
        print("PRODUCT UPDATER - Summary")
        print("=" * 60)
        print(f"  Total Processed: {progress['total_processed']:,}")
        print(f"  Total Updated:   {progress['total_updated']:,}")
        print(f"  Total Errors:    {progress['total_errors']:,}")
        print(f"  Progress saved to: {PROGRESS_FILE}")
        print(f"  Errors saved to:   {ERROR_FILE}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
