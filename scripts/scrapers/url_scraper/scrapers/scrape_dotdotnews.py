import concurrent.futures
import re
import subprocess
from datetime import datetime

from bs4 import BeautifulSoup


def fetch_with_curl(url):
    try:
        # Simulate curl request
        result = subprocess.run(["curl", "-s", "--max-time", "5", "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", url], check=False, capture_output=True, timeout=10)
        if result.returncode != 0:
            return None
        return result.stdout.decode("utf-8", errors="ignore")
    except Exception:
        return None


def scrape():
    base_url = "https://english.dotdotnews.com"
    start_urls = ["https://english.dotdotnews.com/", "https://english.dotdotnews.com/hknews", "https://english.dotdotnews.com/deepline", "https://english.dotdotnews.com/envideo"]
    keywords = ["Wang Fuk", "Tai Po fire", "Tai Po No. 5 fire", "Wang Fuk Court"]

    articles = []
    visited_urls = set()

    def get_soup(url):
        content = fetch_with_curl(url)
        if not content:
            return None
        return BeautifulSoup(content, "html.parser")

    def extract_date(soup):
        text_content = soup.get_text()
        date_match = re.search(r"(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})", text_content)
        if date_match:
            return date_match.group(1)

        meta_date = soup.find("meta", property="article:published_time")
        if meta_date:
            return meta_date["content"]

        return "Unknown Date"

    def parse_article(url):
        if url in visited_urls:
            return
        visited_urls.add(url)

        soup = get_soup(url)
        if not soup:
            return

        title_tag = soup.find("h1")
        if not title_tag:
            title_tag = soup.find("title")

        if not title_tag:
            return

        title = title_tag.get_text().strip()

        is_relevant = False
        for kw in keywords:
            if kw.lower() in title.lower():
                is_relevant = True
                break

        if not is_relevant:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and any(kw.lower() in meta_desc["content"].lower() for kw in keywords):
                is_relevant = True

        if is_relevant:
            date_str = extract_date(soup)
            try:
                date_obj = datetime.strptime(date_str, "%Y.%m.%d %H:%M")
                date_display = date_obj.strftime("%Y-%m-%d")
            except Exception:
                date_display = date_str

            articles.append((date_display, title, url))

    article_links = set()

    print(f"Scanning {len(start_urls)} sections for articles...")
    for start_url in start_urls:
        soup = get_soup(start_url)
        if not soup:
            continue

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/"):
                href = base_url + href
            elif not href.startswith("http"):
                continue

            if "/a/202" in href and ".html" in href:
                article_links.add(href)

    print(f"Found {len(article_links)} potential articles. Processing concurrently...")

    count = 0
    total = len(article_links)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(parse_article, link) for link in article_links]
            for future in concurrent.futures.as_completed(futures):
                count += 1
                if count % 5 == 0:
                    print(f"Processed {count}/{total} articles...")
                try:
                    future.result(timeout=10)
                except Exception as e:
                    print(f"Error processing article: {e}")
    except KeyboardInterrupt:
        print("Scraping interrupted by user. Returning collected articles.")
    except Exception as e:
        print(f"Unexpected error during scraping: {e}")

    unique = []
    seen = set()
    for date, title, url in articles:
        if url not in seen:
            seen.add(url)
            unique.append((date, title, url))

    return ("DotDotNews", unique)
