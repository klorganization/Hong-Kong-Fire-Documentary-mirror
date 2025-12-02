import datetime
import time

import requests
from bs4 import BeautifulSoup


def scrape():
    base_url = "https://www.theguardian.com/world"
    start_date = datetime.date(2025, 11, 26)
    end_date = datetime.date.today()
<<<<<<< Updated upstream
=======
    keywords = ["Hong Kong", "Tai Po", "Wang Fuk Court", "Fire", "Blaze"]
>>>>>>> Stashed changes
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    month_map = {1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun", 7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec"}

    articles = []

    def get_articles_for_date(date_obj):
        month_str = month_map[date_obj.month]
        day_str = date_obj.strftime("%d")
        year_str = date_obj.strftime("%Y")

        url = f"{base_url}/{year_str}/{month_str}/{day_str}/all"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                url_alt = f"{base_url}/{year_str}/{month_str}/{day_str}"
                response = requests.get(url_alt, headers=headers, timeout=10)
                if response.status_code != 200:
                    return []

            soup = BeautifulSoup(response.content, "html.parser")
            found = []
            links = soup.find_all("a", href=True)
            seen_links = set()

            for link in links:
                href = link["href"]
                text = link.get_text(" ", strip=True)

                if not text or not href:
                    continue

                text_lower = text.lower()

                if "hong kong" in text_lower and ("fire" in text_lower or "blaze" in text_lower or "tai po" in text_lower or "wang fuk" in text_lower):
                    if href not in seen_links:
                        found.append((date_obj.strftime("%Y-%m-%d"), text, href))
                        seen_links.add(href)
            return found

        except Exception:
            return []

    current_date = start_date
    while current_date <= end_date:
        found = get_articles_for_date(current_date)
        articles.extend(found)
        current_date += datetime.timedelta(days=1)
        time.sleep(0.5)

    return ("Guardian", articles)
