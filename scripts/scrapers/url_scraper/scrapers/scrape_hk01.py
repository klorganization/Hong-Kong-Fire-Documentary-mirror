import asyncio
import datetime
import re

from playwright.async_api import async_playwright


async def _scrape_async():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu", "--disable-setuid-sandbox"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        page = await context.new_page()

        results = []
        target_url = "https://www.hk01.com/issue/10398"
        print(f"Scraping HK01 Issue Page: {target_url}")

        try:
            # Try multiple wait strategies with shorter timeouts
            try:
                await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                print(f"Warning: Page load issue: {e}")
                # Try a simple goto without wait condition
                await page.goto(target_url, timeout=30000)

            # Wait for content to appear
            try:
                await page.wait_for_selector('a[href*="/"]', timeout=10000)
            except Exception:
                pass

            await asyncio.sleep(3)

            # Scroll to load more content
            for _ in range(3):
                await page.mouse.wheel(0, 3000)
                await asyncio.sleep(2)

            items = await page.evaluate(r"""() => {
                const data = [];
                const seen = new Set();

                document.querySelectorAll('a').forEach(a => {
                    const href = a.href;
                    const title = a.innerText.trim();

                    if (!href || title.length < 5) return;
                    if (seen.has(href)) return;

                    if (!href.match(/\/\d+\//)) return;

                    seen.add(href);

                    let dateStr = "";
                    let parent = a.parentElement;
                    for(let i=0; i<6; i++) {
                        if(!parent) break;
                        const t = parent.querySelector('time') ||
                                  parent.querySelector('.time') ||
                                  parent.querySelector('span[class*="time"]') ||
                                  parent.querySelector('span[class*="date"]');
                        if (t) {
                            dateStr = t.innerText.trim();
                            break;
                        }
                        parent = parent.parentElement;
                    }

                    data.push({title, href, dateStr});
                });
                return data;
            }""")

            print(f"Found {len(items)} items on page.")

            today = datetime.date.today()
            print(f"Processing articles (today is {today})...")

            for item in items:
                title = item["title"]
                link = item["href"]
                date_text = item["dateStr"]

                final_date = None

                # Try to parse the date from the date text first
                if date_text:
                    try:
                        if "分鐘前" in date_text or "小時前" in date_text or "Just now" in date_text:
                            final_date = today.strftime("%Y-%m-%d")
                        elif "昨日" in date_text or "昨天" in date_text:
                            d = today - datetime.timedelta(days=1)
                            final_date = d.strftime("%Y-%m-%d")
                        elif "前" in date_text and "天" in date_text:
                            days_match = re.search(r"(\d+)", date_text)
                            if days_match:
                                days_ago = int(days_match.group(1))
                                d = today - datetime.timedelta(days=days_ago)
                                final_date = d.strftime("%Y-%m-%d")
                        else:
                            # Try various date formats
                            # YYYY-MM-DD or YYYY/MM/DD
                            match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", date_text)
                            if match:
                                final_date = f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
                            else:
                                # DD-MM-YYYY or DD/MM/YYYY
                                match = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", date_text)
                                if match:
                                    final_date = f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
                    except Exception as e:
                        print(f"Error parsing date text '{date_text}': {e}")

                # If we couldn't parse the date from text, try extracting from URL or content
                if not final_date:
                    # Look for timestamp in article ID (HK01 URLs contain article IDs)
                    # Pattern: hk01.com/category/ARTICLEID/title
                    url_match = re.search(r"/(\d{8})\d*/", link)
                    if url_match:
                        # Extract YYYYMMDD from the beginning of article ID
                        date_str = url_match.group(1)
                        try:
                            final_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
                            # Validate the date
                            datetime.datetime.strptime(final_date, "%Y-%m-%d")
                        except Exception:
                            final_date = None

                # Last resort: default to fire incident date
                if not final_date:
                    print(f"Warning: Could not parse date for '{title[:50]}...', date_text='{date_text}', using default")
                    final_date = "2025-11-26"

                # Debug: show first few results
                if len(results) < 5:
                    print(f"  -> [{final_date}] {title[:60]}...")

                results.append((final_date, title, link))

        except Exception as e:
            print(f"Error scraping HK01: {e}")

        await browser.close()
        return results


def scrape():
    try:
        formatted_results = asyncio.run(_scrape_async())
        return ("HK01", formatted_results)
    except Exception as e:
        print(f"Scrape failed: {e}")
        return ("HK01", [])
