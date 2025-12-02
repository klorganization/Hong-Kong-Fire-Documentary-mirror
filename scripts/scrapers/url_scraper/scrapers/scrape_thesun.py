import re
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def scrape():
    search_url = "https://www.thesun.co.uk/?s=Hong+Kong"
    articles = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()

        try:
            page.goto(search_url, timeout=60000, wait_until="domcontentloaded")
            content = page.content()
            soup = BeautifulSoup(content, "html.parser")

            links = soup.select("a.search-results__story-link")
            seen = set()

            potential_articles = []

            for link in links:
                url = link.get("href", "")
                if url in seen:
                    continue
                seen.add(url)

                headline_tag = link.find(class_="search-results__story-headline")
                title = headline_tag.get_text(strip=True) if headline_tag else ""

                combined_text = (title + " " + url).lower()
                if "fire" in combined_text or "blaze" in combined_text or "inferno" in combined_text or "flames" in combined_text:
                    if "hong kong" in combined_text or "hk" in combined_text:
                        potential_articles.append({"title": title, "url": url})

            for art in potential_articles:
                try:
                    page.goto(art["url"], timeout=30000, wait_until="domcontentloaded")

                    published_time = page.evaluate("""() => {
                        const meta = document.querySelector('meta[property="article:published_time"]');
                        return meta ? meta.content : null;
                    }""")

                    date_str = "Unknown"
                    if published_time:
                        try:
                            dt = datetime.fromisoformat(published_time.replace("Z", "+00:00"))
                            date_str = dt.strftime("%Y-%m-%d")
                        except Exception:
                            date_str = published_time.split("T")[0]
                    else:
                        match = re.search(r"/(\d{4})/(\d{2})/", art["url"])
                        if match:
                            date_str = f"{match.group(1)}-{match.group(2)}-??"

                    articles.append((date_str, art["title"], art["url"]))

                except Exception:
                    continue

        except Exception:
            pass
        finally:
            browser.close()

    return ("The Sun", articles)
