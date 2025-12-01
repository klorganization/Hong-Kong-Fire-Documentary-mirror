import re
import time

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def scrape():
    query = "宏福苑"
    target_date_prefix = "202511"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()
        links = []

        try:
            page.goto("https://hk.on.cc/search/index.html", timeout=30000, wait_until="commit")
            try:
                page.wait_for_selector("#center-searchSubject", timeout=10000)
            except Exception:
                pass

            selector = "#center-searchSubject"
            if page.is_visible(selector):
                page.fill(selector, query)
                page.press(selector, "Enter")
                time.sleep(10)

                soup = BeautifulSoup(page.content(), "html.parser")
                found = soup.find_all("a", href=True)

                for a in found:
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if "bkn/cnt/news" in href:
                        links.append({"title": text, "url": href})
        except Exception:
            pass

        anchor_url = None
        if links:
            for link in links:
                if target_date_prefix in link["url"]:
                    anchor_url = link["url"]
                    if anchor_url.startswith("//"):
                        anchor_url = "https:" + anchor_url
                    elif anchor_url.startswith("/"):
                        anchor_url = "https://hk.on.cc" + anchor_url
                    break

        if not anchor_url and "宏福苑" in query:
            anchor_url = "https://hk.on.cc/hk/bkn/cnt/news/20251129/bkn-20251129033525667-1129_00822_001.html"

        if anchor_url:
            try:
                page.goto(anchor_url, timeout=30000, wait_until="domcontentloaded")
                time.sleep(5)

                soup = BeautifulSoup(page.content(), "html.parser")

                title_tag = soup.find("h1")
                if title_tag:
                    links.append({"title": title_tag.get_text(strip=True), "url": anchor_url})

                all_links = soup.find_all("a", href=True)
                for a in all_links:
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if "bkn/cnt/news" in href:
                        if any(k in text for k in query.split()):
                            links.append({"title": text, "url": href})

            except Exception:
                pass

        browser.close()

        unique_articles = {}
        for item in links:
            url = item["url"]
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = "https://hk.on.cc" + url

            if url not in unique_articles:
                date_match = re.search(r"/(\d{8})/", url)
                if date_match:
                    date_str = date_match.group(1)
                    if date_str.startswith(target_date_prefix):
                        unique_articles[url] = (date_str, item["title"], url)

        return ("OnCC", list(unique_articles.values()))
