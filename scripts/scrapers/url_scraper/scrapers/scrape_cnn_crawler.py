import time

import requests
from bs4 import BeautifulSoup


def scrape():
    base_url = "https://edition.cnn.com"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    articles = []
    sections = ["/world/asia", "/china", "/world"]

    def is_relevant(title):
        t_lower = title.lower()
        if "hong kong" in t_lower and ("fire" in t_lower or "blaze" in t_lower):
            return True
        if "tai po" in t_lower:
            return True
        if "wang fuk" in t_lower:
            return True
        return False

    def extract_date_from_url(url):
        try:
            parts = url.strip("/").split("/")
            if len(parts) >= 3:
                year, month, day = parts[0], parts[1], parts[2]
                if year.isdigit() and month.isdigit() and day.isdigit() and len(year) == 4:
                    return f"{year}-{month}-{day}"
        except Exception:
            pass
        return "Unknown"

    for sec in sections:
        url = f"{base_url}{sec}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            headline_spans = soup.find_all(class_="container__headline-text")

            for span in headline_spans:
                title = span.get_text().strip()
                parent_link = span.find_parent("a")

                if not parent_link:
                    continue

                href = parent_link.get("href")
                full_url = base_url + href if href.startswith("/") else href

                if is_relevant(title):
                    date_str = extract_date_from_url(href)
                    articles.append((date_str, title, full_url))

        except Exception:
            pass
        time.sleep(1)

    seen = set()
    unique = []
    for date, title, url in articles:
        if url not in seen:
            seen.add(url)
            unique.append((date, title, url))

    return ("CNN News", unique)
