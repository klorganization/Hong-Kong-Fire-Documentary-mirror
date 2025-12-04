import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def scrape():
    base_url = "http://gba.people.cn/"
    keywords = ["大埔", "宏福苑", "火災", "火警", "火灾", "Tai Po", "Wang Fuk Court", "Fire"]
    all_articles = []

    def extract_date_from_url(url):
        match = re.search(r"/(\d{4})/(\d{2})(\d{2})/", url)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        return None

    def scrape_page(url):
        try:
            response = requests.get(url, timeout=10)
            response.encoding = response.apparent_encoding
            if response.status_code != 200:
                return []
        except Exception:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        articles = []

        for a in soup.find_all("a", href=True):
            link = a["href"]
            title = a.get_text(strip=True)

            if not title or len(title) < 5:
                continue

            full_url = urljoin(base_url, link)
            date = extract_date_from_url(full_url)

            if date:
                if any(k in title for k in keywords):
                    articles.append((date, title, full_url))
        return articles

    pages_to_scrape = ["index.html", "index1.html"] + [f"index{i}.html" for i in range(2, 11)]

    for page in pages_to_scrape:
        url = urljoin(base_url, page)
        articles = scrape_page(url)
        all_articles.extend(articles)

    unique_articles = {}
    for date, title, url in all_articles:
        unique_articles[url] = (date, title, url)

    return ("人民日報大湾区", list(unique_articles.values()))
