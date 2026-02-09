import asyncio
import re
import os
import sys
import json
import argparse
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Cache file for storing detected brand types
BRAND_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brand_types.json")

def ask_url():
    return input("Enter product URL: ").strip()

def get_domain(url):
    """Extract domain from URL (e.g., 'lovegen.com' from 'https://lovegen.com/products/...')"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    # Remove www. prefix if present
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain

def load_brand_cache():
    """Load cached brand types from JSON file."""
    if os.path.exists(BRAND_CACHE_FILE):
        try:
            with open(BRAND_CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_brand_cache(cache):
    """Save brand types cache to JSON file."""
    with open(BRAND_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

class SizeChartScraper:
    def __init__(self, url, headless=True):
        self.url = url
        self.headless = headless
        self.user_data_dir = os.path.abspath("./user_data")
        self.detected_type = None  # Will be set by detect_size_chart_type
        self.domain = get_domain(url)
        self.brand_cache = load_brand_cache()

    async def detect_size_chart_type(self, page):
        """Analyzes the page to detect what type of size chart implementation is used."""
        result = await page.evaluate("""
            () => {
                const types = [];
                
                // Check for Modal libraries
                // iLoveMySize
                if (document.querySelector('.ilm-sizechart-embed, .ilmsc-button, .ilm-sizechart-block')) {
                    types.push({ type: 'MODAL_ILMS', confidence: 90, selector: '.ilmsc-modal' });
                }
                
                // Magnific Popup
                if (document.querySelector('.mfp, .sizelink, [data-mfp-src], a[href*="mfp"]')) {
                    types.push({ type: 'MODAL_MFP', confidence: 85, selector: '.mfp-content' });
                }
                
                // PhotoSwipe
                if (document.querySelector('.pswp, [data-pswp], .t4s-btn__size-chart')) {
                    types.push({ type: 'MODAL_PSWP', confidence: 85, selector: '.pswp__scroll-wrap' });
                }
                
                // Generic modal triggers
                if (document.querySelector('[data-toggle="modal"], [data-bs-toggle="modal"]')) {
                    types.push({ type: 'MODAL_BOOTSTRAP', confidence: 70, selector: '.modal.show' });
                }
                
                // Check for Accordions
                const sizeAccordion = document.querySelector('details summary, collapsible-row');
                if (sizeAccordion) {
                    const text = sizeAccordion.innerText?.toLowerCase() || '';
                    if (text.includes('size')) {
                        types.push({ type: 'ACCORDION', confidence: 80, selector: 'details[open]' });
                    }
                }
                
                // Check for product accordions (Shopify Dawn theme style)
                if (document.querySelector('.product__accordion, .accordion')) {
                    types.push({ type: 'ACCORDION', confidence: 60, selector: 'details[open]' });
                }
                
                // Check for Tab panels
                if (document.querySelector('[role="tablist"], .product-tabs, .tabs')) {
                    const tabTexts = Array.from(document.querySelectorAll('[role="tab"], .tab-button, .tabs button'));
                    for (const tab of tabTexts) {
                        if (/size/i.test(tab.innerText)) {
                            types.push({ type: 'TAB', confidence: 75, selector: '[role="tabpanel"]' });
                            break;
                        }
                    }
                }
                
                // Check for inline size chart (already visible on page)
                // Many patterns: ai-size-chart, size-chart, sizechart, etc.
                const inlineSelectors = [
                    '[class*="size-chart"]',
                    '[class*="sizechart"]', 
                    '[class*="size_chart"]',
                    '[class*="sizeChart"]',
                    '[id*="size-chart"]',
                    '[id*="sizechart"]',
                    '.size-guide-table',
                    'table[class*="size"]'
                ];
                for (const sel of inlineSelectors) {
                    const el = document.querySelector(sel);
                    if (el && (el.offsetWidth > 0 || el.offsetHeight > 0)) {
                        types.push({ 
                            type: 'INLINE', 
                            confidence: 85, 
                            selector: sel,
                            foundElement: el.className?.substring?.(0, 50)
                        });
                        break;
                    }
                }
                
                // Check for Kiwi Sizing Modals
                if (document.querySelector('.ks-chart-container, .kiwi-sizing-modal, .ks-modal-content, [id*="kiwi-sizing"]')) {
                    types.push({ type: 'MODAL_KIWI', confidence: 95, selector: '.kiwi-sizing-modal' });
                }
                // Check iframes for Kiwi Sizing
                const iframes = document.querySelectorAll('iframe');
                for (const iframe of iframes) {
                    if (iframe.src && iframe.src.includes('kiwisizing')) {
                        types.push({ type: 'MODAL_KIWI', confidence: 95, selector: '.kiwi-sizing-modal' });
                        break;
                    }
                }
                
                // Check for size-related links that might be direct image links
                const sizeLinks = document.querySelectorAll('a[href*="size"], a[href*="chart"]');
                for (const link of sizeLinks) {
                    if (/\\.(jpg|png|webp|gif)/i.test(link.href)) {
                        types.push({ type: 'DIRECT_IMAGE', confidence: 65, selector: null, url: link.href });
                        break;
                    }
                }
                
                // Sort by confidence
                types.sort((a, b) => b.confidence - a.confidence);
                
                return {
                    detectedTypes: types,
                    primaryType: types.length > 0 ? types[0] : { type: 'UNKNOWN', confidence: 0 }
                };
            }
        """)
        
        print(f"\n=== SIZE CHART TYPE DETECTION ===")
        for t in result['detectedTypes']:
            print(f"  {t['type']}: confidence={t['confidence']}%")
        print(f"  Primary: {result['primaryType']['type']}\n")
        
        self.detected_type = result['primaryType']['type']
        return result

    async def run(self):
        async with async_playwright() as p:
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

            print(f"Launching browser with user data: {self.user_data_dir}")
            context = await p.chromium.launch_persistent_context(
                self.user_data_dir,
                headless=self.headless,
                args=args,
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )

            page = context.pages[0] if context.pages else await context.new_page()
            
            # Add stealth scripts
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            print(f"Opening {self.url}...")
            try:
                await page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"Error loading page: {e}")
                
            # Allow some dynamic content to hydrate
            await page.wait_for_timeout(2000)

            # 0. DETECT SIZE CHART TYPE (with caching)
            if self.domain in self.brand_cache:
                # Use cached type - FAST PATH
                cached = self.brand_cache[self.domain]
                self.detected_type = cached['type']
                print(f"\n=== USING CACHED TYPE ===")
                print(f"  Domain: {self.domain}")
                print(f"  Type: {self.detected_type} (cached)")
                print(f"  Original confidence: {cached.get('confidence', 'N/A')}%\n")
            else:
                # Detect and cache - SLOW PATH (first time only)
                detection = await self.detect_size_chart_type(page)
                
                # Save to cache for future use
                self.brand_cache[self.domain] = {
                    'type': self.detected_type,
                    'confidence': detection['primaryType'].get('confidence', 0),
                    'selector': detection['primaryType'].get('selector', None)
                }
                save_brand_cache(self.brand_cache)
                print(f"  [CACHE] Saved to cache: {self.domain} -> {self.detected_type}")

            # 1. INTERACTION: Find and Click Triggers (type-aware)
            navigated_away = await self.interact_with_triggers(page)
            
            # Wait for popups/modals to appear - use type-specific wait
            await page.wait_for_timeout(2000)
            
            # Type-specific post-interaction wait
            popup_selector = None
            if self.detected_type == 'MODAL_ILMS':
                popup_selector = '.ilmsc-modal'
            elif self.detected_type == 'MODAL_MFP':
                popup_selector = '.mfp-content'
            elif self.detected_type == 'MODAL_PSWP':
                popup_selector = '.pswp--open'
            elif self.detected_type == 'MODAL_BOOTSTRAP':
                popup_selector = '.modal.show'
            elif self.detected_type == 'MODAL_KIWI':
                popup_selector = '.kiwi-sizing-modal, .ks-chart-container, .ks-modal-content'
            elif self.detected_type == 'ACCORDION':
                popup_selector = 'details[open]'
            elif self.detected_type == 'TAB':
                popup_selector = '[role="tabpanel"]'
            
            if popup_selector:
                try:
                    await page.wait_for_selector(popup_selector, timeout=5000)
                    print(f"Type-specific wait: {popup_selector} appeared")
                    await page.wait_for_timeout(1000)
                except:
                    print(f"Type-specific wait: {popup_selector} did not appear")
            
            # Fallback: Try common popup selectors
            popup_selectors = [
                ".mfp-content", ".ilmsc-modal", ".pswp--open", ".modal.show",
                "[class*='sizechart']", ".size-guide-modal", ".popup-content",
                ".drawer.is-active", ".modal-open", "[aria-modal='true']"
            ]
            for selector in popup_selectors:
                try:
                    if await page.query_selector(selector):
                        print(f"Detected popup: {selector}")
                        await page.wait_for_timeout(1500)
                        break
                except:
                    pass

            # 2. DETECTION & EXTRACTION
            # If navigation occurred, skip DOM extraction and use gallery fallback
            if navigated_away:
                print("Navigation detected - skipping DOM extraction, using gallery fallback only")
                result = {'table': None, 'images': [], 'textHtml': None}
            else:
                result = await self.extract_content(page)
            
            # 3. FALLBACK: Check product gallery images for size chart URLs
            # This catches brands like adlt.in where size chart is in product gallery
            if not result['table'] and not result['images'] and not result.get('textHtml'):
                print("No size chart in DOM. Checking product gallery images...")
                gallery_images = await page.evaluate("""
                    () => {
                        const sizeRegex = /size[_-]?(chart|guide)|measurement|sizing/i;
                        const uniqueImages = new Set();
                        
                        // Helper to clean URL (remove query params and normalize protocol)
                        const cleanUrl = (url) => {
                            try {
                                let clean = url.split('?')[0];
                                if (clean.startsWith('//')) {
                                    clean = 'https:' + clean;
                                }
                                return clean;
                            } catch (e) {
                                return url;
                            }
                        };
                        
                        // Check main product gallery images
                        const galleryImgs = document.querySelectorAll(
                            '.product-media img, .product__media img, .product-single__media img, ' +
                            '.product-image-gallery img, .product-gallery img, [data-product-media] img, ' +
                            '.product__slides img, .slick-slide img, .swiper-slide img'
                        );
                        
                        for (const img of galleryImgs) {
                            const src = img.src || img.dataset?.src || '';
                            if (sizeRegex.test(src)) {
                                uniqueImages.add(cleanUrl(src));
                            }
                        }
                        
                        // Also check all srcset/data-srcset for size chart images
                        const allImgs = document.querySelectorAll('img[srcset], img[data-srcset]');
                        for (const img of allImgs) {
                            const srcset = img.srcset || img.dataset?.srcset || '';
                            if (sizeRegex.test(srcset)) {
                                // Extract highest quality URL from srcset
                                const match = srcset.split(',').pop().trim().split(' ')[0];
                                if (match) {
                                    uniqueImages.add(cleanUrl(match));
                                }
                            }
                        }
                        
                        return Array.from(uniqueImages);
                    }
                """)
                
                if gallery_images:
                    print(f"  Found {len(gallery_images)} size chart image(s) in product gallery!")
                    for img in gallery_images:
                        print(f"    - {img[:80]}...")
                    result['images'] = gallery_images[:3]  # Limit to 3
                else:
                    print("No size chart found in product gallery either.")

            self.print_result(result)
            await context.close()

    async def interact_with_triggers(self, page):
        """Finds and clicks potential size chart triggers."""
        selector = "button, a, span, div, summary, .size-guide, .size-chart, .sizelink, .mfp, collapsible-row"
        
        logs = await page.evaluate("""
            (selector) => {
                const logs = [];
                function log(msg) { logs.push(msg); }
                
                log("Starting interactions...");
                // Primary keywords - specific size chart terms
                const primaryKeywords = /size\\s*(chart|guide|help|specs|match|recommendation|link)|measurements|dimensions|sizing\\s*(chart|guide)|body\\s*(chart|guide)|find\\s*my\\s*size|sizelink/i;
                
                // Secondary keywords - standalone 'size' for accordion titles (must be short text)
                const accordionKeywords = /^size$/i;
                
                const elements = Array.from(document.querySelectorAll(selector));
                log(`Found ${elements.length} candidate elements.`);
                
                // Debug implicit .sizelink
                const sl = document.querySelector('.sizelink');
                if (sl) log(`[DEBUG] .sizelink found. Text: "${sl.textContent}" InnerText: "${sl.innerText}" Class: "${sl.className}"`);
                
                function isClickable(el) {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    const visible = (rect.width > 0 || rect.height > 0 || el.getClientRects().length > 0) && style.visibility !== 'hidden';
                    return visible;
                }

                for (const el of elements) {
                    let textToCheck = "";
                    if (el.tagName.toLowerCase() === 'collapsible-row') {
                         const label = el.querySelector('[slot="heading"], summary, .accordion__title');
                         textToCheck = label ? label.textContent : el.textContent;
                    } else {
                         // Use innerText to ignore hidden spacing, and trim first
                         textToCheck = (el.innerText || el.textContent || "").trim();
                         if (textToCheck.length > 100) continue; 
                    }
                    textToCheck = (textToCheck || "").trim();
                    
                    // Explicit validation to avoid "Sizing is actually accurate"
                    if (/actually\\s*accurate/i.test(textToCheck)) {
                        if (textToCheck.toLowerCase().includes("size")) log(`[SKIP] Excluded phrase: "${textToCheck}"`);
                        continue;
                    }
                    
                    // Check for accordion elements (SUMMARY, DETAILS) - allow standalone "size"
                    const isAccordion = ['summary', 'details'].includes(el.tagName.toLowerCase()) || 
                                       el.closest('details') !== null ||
                                       el.classList.contains('accordion__title') ||
                                       el.classList.contains('summary__title');
                    
                    let match = (textToCheck && primaryKeywords.test(textToCheck)) ||
                                (el.title && primaryKeywords.test(el.title)) ||
                                (el.className && typeof el.className === 'string' && primaryKeywords.test(el.className));
                    
                    // For accordion elements, also match standalone "size"
                    if (!match && isAccordion && accordionKeywords.test(textToCheck)) {
                        match = true;
                        log(`[ACCORDION MATCH] Standalone 'size' in ${el.tagName}`);
                    }
                    
                    if (!match && (textToCheck.toLowerCase().includes("size") || textToCheck.toLowerCase().includes("chart"))) {
                         log(`[MISS] Text "${textToCheck}" did not match regex. Tag: ${el.tagName}`);
                    }

                    if (match) {
                        const clickable = isClickable(el);
                        log(`Match: <${el.tagName}> "${textToCheck}" Class: "${el.className}" Clickable: ${clickable}`);
                        
                        if (clickable) {
                            el.setAttribute("data-sc-trigger", "true");
                            log(`[ACTION] Clicking trigger: <${el.tagName}>`);

                            if (el.tagName.toLowerCase() === 'collapsible-row') {
                                const summary = el.querySelector('summary, button');
                                if (summary) summary.click();
                                el.setAttribute("data-sc-priority", "true");
                            } else if (el.tagName.toLowerCase() === 'summary') {
                                // Handle summary inside details - expand the details
                                try { el.click(); } catch(e) { log(`Click error: ${e}`); }
                                const parentDetails = el.closest('details');
                                if (parentDetails) {
                                    parentDetails.setAttribute('open', 'true');
                                    parentDetails.setAttribute("data-sc-priority", "true");
                                    log(`[ACCORDION] Opened details element`);
                                }
                            } else if (el.tagName.toLowerCase() === 'details') {
                                // Directly clicked on details - open it
                                el.setAttribute('open', 'true');
                                el.setAttribute("data-sc-priority", "true");
                                const summary = el.querySelector('summary');
                                if (summary) summary.click();
                                log(`[ACCORDION] Opened details element directly`);
                            } else {
                                // Check for aria-controls or data-target to identify specific modal
                                const targetId = el.getAttribute('aria-controls') || el.getAttribute('data-target') || el.getAttribute('href');
                                if (targetId) {
                                    const cleanId = targetId.replace('#', '');
                                    window._sc_target_modal_id = cleanId;
                                    log(`[TARGET] Set target modal ID to: ${cleanId}`);
                                }

                                try { el.click(); } catch(e) { log(`Click error: ${e}`); }
                                
                                // Check for MFP (Magnific Popup)
                                if (el.className.includes('mfp') || el.className.includes('sizelink')) {
                                    log("[DEBUG] Clicked MFP trigger, setting flag...");
                                    window._sc_mfp_clicked = true;
                                }
                                
                                // Propagate priority
                                let p = el.parentElement;
                                while(p && p !== document.body) {
                                    const t = p.tagName.toLowerCase();
                                    const c = (p.className || "").toString();
                                    if (t === 'details' || t === 'dialog' || c.includes('modal') || c.includes('popup') || c.includes('drawer')) {
                                        p.setAttribute("data-sc-priority", "true");
                                        if (t === 'details') p.setAttribute('open', 'true');
                                        break;
                                    }
                                    p = p.parentElement;
                                }
                            }
                        }
                    }
                }
                return logs;
            }
        """, selector)
        
        for msg in logs:
            print(f"INTERACTION LOG: {msg}")

        # Post-interaction wait for MFP - wrapped in try-except to handle navigation
        try:
            is_mfp = await page.evaluate("() => window._sc_mfp_clicked === true")
            if is_mfp:
                print("MFP Trigger clicked. Waiting for .mfp-content specifically...")
                try:
                    await page.wait_for_selector(".mfp-content", timeout=5000)
                    print("MFP Content appeared!")
                    await page.wait_for_timeout(2000)
                except:
                    print("MFP Content did not appear within timeout.")
            return False  # No navigation occurred
        except Exception as e:
            # Navigation may have occurred (e.g., clicking a link that goes to size guide page)
            if "destroyed" in str(e) or "navigation" in str(e).lower():
                print("INTERACTION LOG: Page navigated after click - going back to product page")
                try:
                    await page.go_back()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(2000)
                    print("INTERACTION LOG: Returned to product page - will use gallery fallback")
                except:
                    print("INTERACTION LOG: Could not go back - will try gallery fallback anyway")
                return True  # Navigation occurred, use gallery fallback
            return False

    async def extract_content(self, page):
        """Identifies best container and extracts images/tables."""
        
        # First, force lazy-loaded images to load
        await page.evaluate("""
            () => {
                // Force lazy images to load
                document.querySelectorAll('img[data-src], img[data-srcset], img[loading="lazy"]').forEach(img => {
                    if (img.dataset.src) img.src = img.dataset.src;
                    if (img.dataset.srcset) img.srcset = img.dataset.srcset;
                    img.loading = 'eager';
                });
                // Also check for noscript images (common lazy pattern)
                document.querySelectorAll('noscript').forEach(ns => {
                    const match = ns.innerHTML.match(/src=["']([^"']+)["']/);
                    if (match && ns.previousElementSibling?.tagName === 'IMG') {
                        ns.previousElementSibling.src = match[1];
                    }
                });
            }
        """)
        
        # Wait for images to load
        await page.wait_for_timeout(1500)
        
        # MODAL-FIRST EXTRACTION: Check known modal selectors first
        modal_result = await page.evaluate("""
            () => {
                const logs = [];
                function log(msg) { logs.push(msg); }
                const keywords = /size\\s*(chart|guide|help|specs|match|recommendation|link)|measurements|dimensions|sizing|t-shirt|womens|mens/i;
                
                // Priority modal selectors - extract directly from these if they exist and are visible
                const modalSelectors = [
                    // JSC Size Chart app modals
                    '.jsc-modal', '.jsc-modal-body', '.jsc-modal-content', '.jsc-size-chart-modal',
                    '[class*="jsc-"]',
                    // Kiwi Sizing Modals
                    '.kiwi-sizing-modal', '.ks-chart-container', '.ks-modal-content', '[id*="kiwi-sizing"]',
                    // Other size chart modals
                    '.ilmsc-modal', '.ilmsc-modal-content', '.ilmsc-content', '.sizechart-container',
                    '.mfp-content', '.mfp-container', 
                    '.pswp--open .pswp__scroll-wrap', '.pswp__container', '.pswp__item',
                    '[class*="sizechart-modal"]', '[class*="size-chart-modal"]',
                    '.modal.show .modal-body', '.drawer.is-active', '[aria-modal="true"]'
                ];
                
                let modalContainer = null;
                const modalTargetId = window._sc_target_modal_id;
                
                // 1. Try to find modal by target ID if available
                if (modalTargetId) {
                    log(`[TARGET] Looking for modal with ID: ${modalTargetId}`);
                    modalContainer = document.getElementById(modalTargetId);
                    if (!modalContainer) {
                        // Sometimes the ID is on a child div or parent wrapped in a dialog
                        modalContainer = document.querySelector(`[id*="${modalTargetId}"]`);
                    }
                }
                
                // 2. Fallback to generic search if no target ID or target not found
                if (!modalContainer) {
                    log(`[SEARCH] Searching for active modal (no target ID match)...`);
                    // Prioritize product-popup-modal class as it's specific
                    modalContainer = document.querySelector('.product-popup-modal__content, .product-popup-modal') ||
                                     document.querySelector('[aria-modal="true"][role="dialog"]') || 
                                     document.querySelector('.is-open[role="dialog"]') ||
                                     document.querySelector('.modal.open') ||
                                     document.querySelector('.fancybox-content');
                }
                
                // 3. If still no modal, try the original modalSelectors list
                if (!modalContainer) {
                    for (const sel of modalSelectors) {
                        const el = document.querySelector(sel);
                        // Check for visibility and size to ensure it's an active modal
                        if (el && (el.offsetWidth > 100 || el.offsetHeight > 100)) {
                            log(`Found active modal via generic selector: ${sel}`);
                            modalContainer = el;
                            break;
                        }
                    }
                }

                if (!modalContainer) {
                    log("No active modal found, will use general extraction.");
                    return { table: null, images: [], logs: logs, foundModal: false };
                }
                
                // For JSC modals, prefer the body-wrapper for content extraction
                const jscBody = modalContainer.querySelector('.jsc-modal-body-wrapper, .jsc-modal-body, .jsc-modal-content');
                if (jscBody) {
                    log(`Using JSC body container: ${jscBody.className?.substring?.(0, 50)}`);
                    modalContainer = jscBody;
                }

                // For Kiwi Sizing modals, prefer the chart container
                const kiwiBody = modalContainer.querySelector('.ks-chart-container, .kiwi-sizing-modal-inner, .ks-modal-content');
                if (kiwiBody) {
                    log(`Using Kiwi body container: ${kiwiBody.className?.substring?.(0, 50)}`);
                    modalContainer = kiwiBody;
                }
                
                log(`Extracting from modal container: ${modalContainer.className?.substring?.(0, 50)}`);
                
                // Extract images from modal
                const imgs = Array.from(modalContainer.querySelectorAll("img"));
                log(`Found ${imgs.length} images in modal.`);
                
                const validImages = [];
                const seen = new Set();
                
                for (const img of imgs) {
                    let src = img.src || img.dataset?.src || "";
                    if (!src || src.includes('data:image') || src.includes('placeholder')) continue;
                    if (src.includes('logo') || src.includes('icon')) continue; // Skip logos
                    
                    let score = 0;
                    const srcLower = src.toLowerCase();
                    const alt = (img.alt || "").toLowerCase();
                    
                    if (keywords.test(srcLower) || keywords.test(alt)) score += 50;
                    if (srcLower.includes('sizeguide') || srcLower.includes('size_guide') || srcLower.includes('size-chart')) score += 40;
                    if (srcLower.includes('chart') || srcLower.includes('measurement')) score += 30; 
                    
                    const naturalWidth = img.naturalWidth || parseInt(img.width) || 0;
                    const naturalHeight = img.naturalHeight || parseInt(img.height) || 0;
                    
                    // INSIDE A SIZE CHART MODAL: Accept all images since the modal already confirms it's size chart context
                    // Only skip very small images (icons, etc)
                    if (!seen.has(src) && (naturalWidth > 50 || naturalHeight > 50 || score > 0)) {
                        validImages.push({ src, score: score || 10, w: naturalWidth, h: naturalHeight });
                        seen.add(src);
                        log(`Modal image: ${src.substring(0, 60)}... Score: ${score || 10} Size: ${naturalWidth}x${naturalHeight}`);
                    } else if (!seen.has(src)) {
                        log(`Modal skipped (too small): ${src.substring(0, 60)}...`);
                    }
                }
                
                // Extract tables from modal
                const tables = Array.from(modalContainer.querySelectorAll("table"));
                log(`Found ${tables.length} tables in modal.`);
                let allTables = [];
                for (const table of tables) {
                    const rows = Array.from(table.rows).map(row => 
                        Array.from(row.cells).map(cell => cell.innerText.trim())
                    );
                    if (rows.length > 0 && rows[0].length > 0) {
                        log(`Table with ${rows.length} rows, ${rows[0].length} cols`);
                        allTables.push(rows);
                    }
                }
                
                // Also try to find div-based size charts (common in JSC apps)
                if (allTables.length === 0) {
                    const sizeRows = modalContainer.querySelectorAll('[class*="size-row"], [class*="jsc-row"], .jsc-table-row');
                    if (sizeRows.length > 0) {
                        log(`Found ${sizeRows.length} div-based size rows`);
                    }
                }
                
                validImages.sort((a,b) => b.score - a.score);
                // Limit to max 3 images
                const topImages = validImages.slice(0, 3);
                return { 
                    table: allTables.length ? allTables[0] : null, 
                    images: topImages.map(i => i.src), 
                    logs: logs, 
                    foundModal: true 
                };
            }
        """)
        
        for msg in modal_result.get('logs', []):
            print(f"MODAL EXTRACT LOG: {msg}")
        
        # If modal extraction succeeded, use its results
        if modal_result.get('foundModal') and (modal_result.get('images') or modal_result.get('table')):
            print(f"Using modal extraction: {len(modal_result.get('images', []))} images, {1 if modal_result.get('table') else 0} table")
            return modal_result
        
        # Fall back to general container extraction
        result = await page.evaluate("""
            async () => {
                const logs = [];
                function log(msg) { logs.push(msg); }
                const keywords = /size\\s*(chart|guide|help|specs|match|recommendation|link)|measurements|dimensions|sizing\\s*(chart|guide)|body\\s*(chart|guide)|find\\s*my\\s*size|sizelink|sizechart/i;
                
                function isVisible(el) {
                    const style = window.getComputedStyle(el);
                    return !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length) && style.display !== 'none' && style.visibility !== 'hidden';
                }

                function getScore(el) {
                    let score = 0;
                    const cls = (el.className || "").toString().toLowerCase();
                    const id = (el.id || "").toLowerCase();
                    const text = (el.innerText || "").toLowerCase();
                    const tag = el.tagName.toLowerCase();
                    
                    // Penalize buttons/links as containers - they're triggers, not content
                    if (tag === 'button' || tag === 'a') score -= 100;
                    
                    // HIGH PRIORITY: Explicit size chart containers (inline charts)
                    // These should almost always win over generic sections
                    if (cls.includes("ai-size-chart") || cls.includes("ai_size_chart")) score += 200;
                    if (/size-chart|sizechart|size_chart/i.test(cls) && !cls.includes('button') && !cls.includes('btn')) score += 150;
                    if (/size-chart|sizechart|size_chart/i.test(id)) score += 150;
                    
                    // Prioritize actual popup/modal containers
                    if (cls.includes("pswp") || cls.includes("photoswipe")) score += 80;
                    if (cls.includes("mfp-content") || cls.includes("mfp-wrap")) score += 70;
                    if (cls.includes("modal-content") || cls.includes("modal-body")) score += 60;
                    if (cls.includes("ilmsc-modal") || cls.includes("ilmsc-content")) score += 80;
                    
                    if (el.hasAttribute("data-sc-priority")) score += 50;
                    if (el.hasAttribute("data-sc-trigger")) score -= 20; // Triggers are not content

                    // Class-based scoring for other size-related elements
                    if (cls.includes("sizechart-container") || cls.includes("size-chart-content")) score += 60;
                    if (cls.includes("size") && (cls.includes("chart") || cls.includes("guide"))) score += 30;
                    if (cls.includes("ilm-sizechart") || cls.includes("ilmsc")) score += 50;
                    
                    // Tables in size chart containers are very valuable
                    const tables = el.querySelectorAll("table");
                    if (tables.length > 0) {
                        // Check if table looks like a size chart (has size-related headers)
                        for (const table of tables) {
                            const headerText = (table.rows[0]?.innerText || "").toLowerCase();
                            if (/size|chest|waist|length|width|measurement/i.test(headerText)) {
                                score += 80; // Size table - very high priority
                            } else {
                                score += 25; // Generic table
                            }
                        }
                    }
                    
                    // Images with size-related names - boost only for relevant images
                    const imgs = el.querySelectorAll("img");
                    let sizeRelevantImages = 0;
                    for (const img of imgs) {
                        const src = (img.src || img.dataset?.src || "").toLowerCase();
                        const alt = (img.alt || "").toLowerCase();
                        if (keywords.test(src) || keywords.test(alt)) {
                            score += 40;
                            sizeRelevantImages++;
                        }
                    }
                    
                    // Penalize containers with many generic images (likely product galleries)
                    if (imgs.length > 5 && sizeRelevantImages === 0) score -= 50;
                    
                    // Text content boost - but only if not too large (avoid body elements)
                    if (text.length < 5000 && keywords.test(text.substring(0, 300))) score += 15;
                    
                    // Size penalty for very small elements
                    const rect = el.getBoundingClientRect();
                    if (rect.width < 50 || rect.height < 50) score -= 30;
                    
                    return score;
                }

                // Extended selector list
                const candidates = Array.from(document.querySelectorAll(`
                    div, section, modal-dialog, details, 
                    .product-popup, [data-sc-priority], .mfp-content, 
                    .product-block, .product__info-container,
                    [class*="sizechart"], [class*="size-chart"], [class*="ilm"],
                    .modal-content, .drawer, .popup-content
                `));
                
                let bestContainer = null;
                let maxScore = -1;

                for (const cand of candidates) {
                    if (!isVisible(cand)) continue;
                    const score = getScore(cand);
                    if (score > 15) log(`Candidate <${cand.tagName}> .${cand.className?.substring?.(0, 50)} Score: ${score}`);
                    if (score > maxScore) {
                        maxScore = score;
                        bestContainer = cand;
                    }
                }
                
                if (!bestContainer || maxScore < 10) {
                    log("No best container found.");
                    return { table: null, images: [], logs: logs };
                }
                
                log(`Best Container: <${bestContainer.tagName}> .${bestContainer.className?.substring?.(0, 50)} Score: ${maxScore}`);

                // Extract Tables
                let tables = Array.from(bestContainer.querySelectorAll("table"));
                let allTables = [];
                for (const table of tables) {
                    const rows = Array.from(table.rows).map(row => 
                        Array.from(row.cells).map(cell => cell.innerText.trim())
                    );
                    if (rows.length > 0 && rows[0].length > 0) {
                        allTables.push(rows);
                    }
                }
                
                // Extract Images - comprehensive approach
                const allImgs = Array.from(bestContainer.querySelectorAll("img"));
                log(`Found ${allImgs.length} images in best container.`);
                
                const validImages = [];
                const seen = new Set();
                
                for (const img of allImgs) {
                     // Get src from multiple possible attributes
                     let src = img.src || img.dataset?.src || img.dataset?.lazySrc || "";
                     if (!src && img.srcset) {
                         src = img.srcset.split(',')[0].trim().split(' ')[0];
                     }
                     
                     if (!src || src.includes('data:image') || src.includes('placeholder')) continue;
                     
                     let score = 0;
                     const srcLower = src.toLowerCase();
                     const alt = (img.alt || "").toLowerCase();
                     
                     // Keyword matching
                     if (keywords.test(srcLower) || keywords.test(alt)) score += 30;
                     if (srcLower.includes('size') || srcLower.includes('chart') || srcLower.includes('guide')) score += 20;
                     if (srcLower.includes('measurement') || srcLower.includes('dimension')) score += 20;
                     
                     // Container context boost
                     if (bestContainer.className?.includes?.("sizechart") || bestContainer.className?.includes?.("ilm")) score += 20;
                     if (bestContainer.className?.includes?.("mfp-content")) score += 15;
                     
                     // Size validation - more lenient
                     const naturalWidth = img.naturalWidth || parseInt(img.width) || 0;
                     const naturalHeight = img.naturalHeight || parseInt(img.height) || 0;
                     const hasSize = naturalWidth > 30 || naturalHeight > 30;
                     
                     if (!seen.has(src)) {
                         // ONLY include images with size-related keywords (score > 0)
                         // Ignore generic product images with score 0
                         if (score > 0) {
                             validImages.push({ src: src, score: score, w: naturalWidth, h: naturalHeight });
                             seen.add(src);
                             log(`Image: ${src.substring(0, 60)}... Score: ${score} Size: ${naturalWidth}x${naturalHeight}`);
                         } else {
                             log(`Skipped (no size keywords, score=0): ${src.substring(0, 60)}...`);
                         }
                     }
                }
                
                // Also check for iframes with size chart content
                const iframes = bestContainer.querySelectorAll("iframe");
                for (const iframe of iframes) {
                    try {
                        const iframeSrc = iframe.src || "";
                        if (keywords.test(iframeSrc)) {
                            log(`Found size chart iframe: ${iframeSrc}`);
                            validImages.push({ src: iframeSrc, score: 50, w: 0, h: 0 });
                        }
                    } catch(e) {}
                }
                
                validImages.sort((a,b) => b.score - a.score);
                // Limit to max 3 images to avoid returning too many
                const topImages = validImages.slice(0, 3);
                const finalImages = topImages.map(i => i.src);
                
                // If no images or tables found, try to extract text/HTML content
                // ONLY for specific domains that use text-only size info (like dopamean.in)
                let textHtml = null;
                const textOnlyBrands = ['dopamean.in'];
                const currentDomain = window.location.hostname.replace('www.', '');
                
                if (finalImages.length === 0 && allTables.length === 0 && bestContainer && textOnlyBrands.includes(currentDomain)) {
                    // Check if container has meaningful text content with size keywords
                    const textContent = bestContainer.innerText || "";
                    log(`Text content length: ${textContent.length}, preview: ${textContent.substring(0, 80).replace(/\\n/g, ' ')}...`);
                    
                    // More inclusive regex for size-related content
                    const sizeKeywords = /\\b(size|cm|inch|chest|waist|length|shoulder|sleeve|fit|measurements?|dimensions?|small|medium|large|xs|xl|xxl|armhole|bust|hip)\\b/i;
                    if (sizeKeywords.test(textContent) && textContent.length > 20 && textContent.length < 5000) {
                        // Clean and return the HTML
                        textHtml = bestContainer.innerHTML;
                        log(`Found text-based size info: ${textContent.substring(0, 100).replace(/\\n/g, ' ')}...`);
                    } else {
                        log(`Text does not match size keywords or length requirements`);
                    }
                }

                return { table: allTables.length ? allTables[0] : null, images: finalImages, textHtml: textHtml, logs: logs };
            }
        """)
        
        for msg in result.get('logs', []):
            print(f"EXTRACT LOG: {msg}")
            
        return result

    async def fetch_shopify_fallback(self, page):
        """Fetches Shopify .js JSON and returns last 2 images as fallback."""
        try:
            js_url = self.url.split('?')[0].rstrip('/') + '.js'
            response = await page.request.get(js_url)
            if response.status == 200:
                data = await response.json()
                images = data.get('images', [])
                if len(images) > 1:
                    return [
                        images[-1] if isinstance(images[-1], str) else images[-1].get('src'),
                        images[-2] if isinstance(images[-2], str) else images[-2].get('src')
                    ]
                elif len(images) > 0:
                        return [images[-1] if isinstance(images[-1], str) else images[-1].get('src')]
        except Exception as e:
            print(f"JSON Fallback failed: {e}")
        return []

    def print_result(self, result):
        print("\n====== SIZE CHART RESULT ======\n")
        found = False
        if result['table']:
            print(f"[TABLE] Found Table (Data):")
            for row in result['table']:
                print(" | ".join(row))
            print("\n")
            found = True
        
        if result['images']:
            print(f"[IMAGES] Found Images (Ranked):")
            for img in result['images']:
                print(img)
            print("\n")
            found = True
        
        if result.get('textHtml'):
            print(f"[TEXT HTML] Found Text-based Size Info:")
            # Show first 500 chars of HTML
            html_preview = result['textHtml'][:500] + "..." if len(result['textHtml']) > 500 else result['textHtml']
            print(html_preview)
            print(f"\n[HTML Length: {len(result['textHtml'])} chars]")
            found = True
            
        if not found:
            print("[NONE] No size chart found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Size Chart Scraper")
    parser.add_argument("url", nargs="?", help="Product URL")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode", default=True)
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run in visible mode")
    
    args = parser.parse_args()
    
    url = args.url
    if not url:
        url = ask_url()
        
    scraper = SizeChartScraper(url, headless=args.headless)
    asyncio.run(scraper.run())
