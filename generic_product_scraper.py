import asyncio
import json
import re
from playwright.async_api import async_playwright
import sys

# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

class GenericProductScraper:
    def __init__(self, url, headless=True):
        self.url = url
        self.headless = headless

    async def scrape(self):
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            try:
                print(f"Opening {self.url}...")
                response = await page.goto(self.url, timeout=60000, wait_until="domcontentloaded")
                
                # Check for 404
                if response and response.status == 404:
                    print(f"Server returned 404 for {self.url}")
                    raise Exception("404 Not Found")
                
                await page.wait_for_timeout(3000) # Wait for dynamic content
                
                # Extract Data using JavaScript for robust DOM access
                data = await page.evaluate("""() => {
                    const result = {
                        title: "",
                        price: 0,
                        original_price: 0,
                        description: "",
                        images: [],
                        currency: "INR"
                    };

                    // 1. TITLE
                    const h1 = document.querySelector('h1');
                    if (h1) result.title = h1.innerText.trim();
                    else {
                        const titleMeta = document.querySelector('meta[property="og:title"]');
                        if (titleMeta) result.title = titleMeta.content;
                    }

                    // 2. PRICE & ORIGINAL PRICE
                    
                    // Helper to clean price text
                    const cleanPrice = (text) => {
                        if (!text) return 0;
                        // Remove currency symbols and non-numeric characters except dot
                        const match = text.match(/[0-9,.]+/);
                        if (!match) return 0;
                        // Handle comma as thousand separator
                        return parseFloat(match[0].replace(/,/g, ''));
                    };

                    // A. Try to find WooCommerce Price Structure (most reliable for this site)
                    // Look for standard WooCommerce price block: <p class="price"> ... </p>
                    const wooPrice = document.querySelector('.price, .woocommerce-Price-amount');
                    if (wooPrice) {
                        // Check for sale price structure: <del>Original</del> <ins>Sale</ins>
                        const ins = document.querySelector('.price ins .amount, ins .woocommerce-Price-amount');
                        const del = document.querySelector('.price del .amount, del .woocommerce-Price-amount');
                        
                        if (ins && del) {
                            result.price = cleanPrice(ins.innerText);
                            result.original_price = cleanPrice(del.innerText);
                        } else {
                            // Single price
                            const amount = document.querySelector('.price .amount, .woocommerce-Price-amount');
                            if (amount) {
                                result.price = cleanPrice(amount.innerText);
                            }
                        }
                    }

                    // B. Schema Fallback (only if A failed to find valid price)
                    if (!result.price) {
                        const schemaScripts = document.querySelectorAll('script[type="application/ld+json"]');
                        for (const script of schemaScripts) {
                            try {
                                const json = JSON.parse(script.innerText);
                                const product = Array.isArray(json) ? json.find(i => i['@type'] === 'Product') : (json['@type'] === 'Product' ? json : null);
                                if (product) {
                                    if (product.offers) {
                                        const offer = Array.isArray(product.offers) ? product.offers[0] : product.offers;
                                        // Prefer lowPrice if available (sale price range)
                                        const price = offer.lowPrice || offer.price || offer.highPrice;
                                        result.price = parseFloat(price);
                                        result.currency = offer.priceCurrency || "INR";
                                    }
                                    if (!result.title && product.name) result.title = product.name;
                                    if (!result.description && product.description) result.description = product.description;
                                    if (product.image) {
                                         if (Array.isArray(product.image)) result.images = product.image;
                                         else if (typeof product.image === 'string') result.images = [product.image];
                                         else if (product.image.url) result.images = [product.image.url];
                                    }
                                }
                            } catch (e) {}
                        }
                    }

                    // C. General Fallback
                    if (!result.price) {
                        const priceEls = document.querySelectorAll('.current-price, [class*="price"]');
                        for (const el of priceEls) {
                            const text = el.innerText.trim();
                            if (text.match(/[0-9]/) && !el.closest('del') && !el.closest('.original-price')) {
                                const price = cleanPrice(text);
                                if (price > 0) {
                                    result.price = price;
                                    break;
                                }
                            }
                        }
                    }

                    // Correct original price if it was missed
                    if (!result.original_price) {
                        const delEls = document.querySelectorAll('del, .original-price, .compare-price, .strikethrough');
                        for (const el of delEls) {
                            const price = cleanPrice(el.innerText);
                            if (price > result.price) {
                                result.original_price = price;
                                break;
                            }
                        }
                    }

                    // 3. DESCRIPTION
                    if (!result.description) {
                        const descEls = document.querySelectorAll('.product-description, .description, #description, .woocommerce-product-details__short-description');
                        for (const el of descEls) {
                            if (el.innerText.length > 50) {
                                result.description = el.innerHTML; // Keep HTML for description
                                break;
                            }
                        }
                    }
                    // Meta desc fallback
                    if (!result.description) {
                        const metaDesc = document.querySelector('meta[name="description"]');
                        if (metaDesc) result.description = metaDesc.content;
                    }

                    // 4. IMAGES
                    if (result.images.length === 0) {
                         // Look for gallery images
                        const galleryImgs = document.querySelectorAll('.product-gallery img, .woocommerce-product-gallery__image img, .images img');
                        const imgSet = new Set();
                        
                        // Main image
                        const mainImg = document.querySelector('img.wp-post-image');
                        if (mainImg) {
                            imgSet.add(mainImg.src || mainImg.dataset.src);
                        }

                        for (const img of galleryImgs) {
                            const src = img.src || img.dataset.src;
                            if (src && src.startsWith('http')) imgSet.add(src);
                        }
                        
                        if (imgSet.size === 0) {
                             // Fallback to all large images
                             document.querySelectorAll('img').forEach(img => {
                                 if (img.width > 300 && img.height > 300) imgSet.add(img.src);
                             });
                        }
                        result.images = Array.from(imgSet);
                    }

                    return result;
                }""")
                
                print(f"Scraped Data: {json.dumps(data, indent=2)}")
                return data

            except Exception as e:
                print(f"Error scraping {self.url}: {e}")
                return None
            finally:
                await browser.close()

async def fetch_generic_product_data(url):
    scraper = GenericProductScraper(url)
    return await scraper.scrape()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Product URL")
    args = parser.parse_args()
    asyncio.run(fetch_generic_product_data(args.url))
