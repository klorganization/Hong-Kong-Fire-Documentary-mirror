import asyncio
import datetime
import re

from playwright.async_api import async_playwright


def parse_relative_date(text):
    """
    Parses relative date strings (e.g., 9小時前, 1天前) into YYYY-MM-DD.
    """
    base_date = datetime.date.today()
    clean_text = text.strip()

    # 1. Relative Time (e.g., 2小時前, 1日前, 30分鐘前)
    time_match = re.search(r"(\d+)(小時|分鐘|日|天)前", clean_text)
    if time_match:
        val = int(time_match.group(1))
        unit = time_match.group(2)

        if unit in ["日", "天"]:
            return base_date - datetime.timedelta(days=val)
        return base_date  # Hours/Minutes ago is today

    # 2. Absolute Date YYYY/MM/DD or YYYY-MM-DD
    iso_match = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", clean_text)
    if iso_match:
        y, m, d = map(int, iso_match.groups())
        return datetime.date(y, m, d)

    return base_date


async def _scrape_async():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()

        base_url = "https://news.tvbs.com.tw/pack/packdetail/1463"
        all_results = []
        seen_links = set()

        current_page = 1

        while True:
            if current_page == 1:
                url = base_url
            else:
                url = f"{base_url}/{current_page}"

            print(f"Scraping page {current_page}: {url}")

            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if response.status == 404:
                    print("Reached 404.")
                    break

                # Wait for list to appear
                try:
                    await page.wait_for_selector("li a div.txt_box", timeout=5000)
<<<<<<< Updated upstream
                except Exception:
=======
                except:
>>>>>>> Stashed changes
                    print("Timeout waiting for list content.")
                    break

                # Extract articles
                articles = await page.evaluate("""() => {
                    const results = [];
                    const items = document.querySelectorAll('li a');

                    items.forEach(a => {
                        const txtBox = a.querySelector('div.txt_box');
                        if (!txtBox) return;

                        const titleEl = txtBox.querySelector('h2.txt');
                        if (!titleEl) return;

                        const timeEl = txtBox.querySelector('div.time');
                        const dateStr = timeEl ? timeEl.innerText.trim() : "";

                        results.push({
                            title: titleEl.innerText.trim(),
                            link: a.href,
                            dateStr: dateStr
                        });
                    });
                    return results;
                }""")

                if not articles:
                    print("No articles found on this page.")
                    break

                new_items_found = False
                for item in articles:
                    link = item["link"]
                    if link in seen_links:
                        continue

                    date_obj = parse_relative_date(item["dateStr"])
                    date_fmt = date_obj.strftime("%Y-%m-%d")

                    all_results.append({"date": date_fmt, "title": item["title"], "link": link})
                    seen_links.add(link)
                    new_items_found = True

                if not new_items_found:
                    print("No new items found, stopping.")
                    break

                current_page += 1
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Error on page {current_page}: {e}")
                break

        await browser.close()
        return all_results


def scrape():
    try:
        raw_results = asyncio.run(_scrape_async())
    except Exception as e:
        print(f"TVBS Scraper failed: {e}")
        raw_results = []

<<<<<<< Updated upstream
    formatted_results = [(r["date"], r["title"], r["link"]) for r in raw_results]
=======
    formatted_results = []
    for r in raw_results:
        formatted_results.append((r["date"], r["title"], r["link"]))
>>>>>>> Stashed changes

    return ("TVBS News", formatted_results)


if __name__ == "__main__":
    name, res = scrape()
    print(f"Source: {name}")
    for date, title, link in res:
        print(f"[{date}] {title} ({link})")
