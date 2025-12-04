import re
import time

import requests
from bs4 import BeautifulSoup


def scrape():
    base_url = "https://skypost.hk"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

    urls_to_check = ["https://skypost.hk/", "https://skypost.hk/sras001/港聞"]

    found_articles = {}

    def get_article_date(url):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            match = re.search(r'var\s+pubDate\s*=\s*["\'](.*?)["\'];', response.text)
            if match:
                raw_date = match.group(1)
                parts = raw_date.split("/")
                return f"{parts[0]}年{parts[1]}月{parts[2]}日"

            soup = BeautifulSoup(response.content, "html.parser")
            meta_date = soup.find("meta", property="article:published_time")
            if meta_date:
                content = meta_date.get("content")
                match = re.search(r"(\d{4}-\d{2}-\d{2})", content)
                if match:
                    parts = match.group(1).split("-")
                    return f"{parts[0]}年{parts[1]}月{parts[2]}日"

        except Exception:
            pass

        return "Unknown Date"

    for url in urls_to_check:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            cards = soup.find_all("div", class_="card")

            for card in cards:
                title_tag = card.select_one("h5.card-title a")
                if not title_tag:
                    continue

                title = title_tag.get_text(strip=True)
                link = title_tag["href"]
                if not link.startswith("http"):
                    link = base_url + link

                if link in found_articles:
                    continue

                if ("大埔" in title and "火" in title) or "宏福苑" in title:
                    date_text = get_article_date(link)

                    if "2025年11月" in date_text or "2025年12月" in date_text:
                        found_articles[link] = (date_text, title, link)

                    time.sleep(0.5)

        except Exception:
            pass

    return ("晴報 (Sky Post)", list(found_articles.values()))
