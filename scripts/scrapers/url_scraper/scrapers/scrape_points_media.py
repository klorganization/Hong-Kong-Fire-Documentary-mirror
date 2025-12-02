import re
import time

import requests
from bs4 import BeautifulSoup


def scrape():
    base_url = "https://points-media.com/"
    search_query = "宏福苑"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

    all_articles = {}
    page = 1

    while True:
        params = {"s": search_query}
        if page > 1:
            params["paged"] = page

        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=10)
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, "html.parser")
            article_titles = soup.find_all(["h1", "h2", "h3"])

            if not article_titles:
                break

            for title_tag in article_titles:
                link_tag = title_tag.find("a")
                if not link_tag:
                    continue

                title_text = link_tag.get_text(strip=True)
                title_attr = link_tag.get("title")

                final_title = title_text
                if title_attr and len(title_attr) > len(title_text):
                    final_title = title_attr

                link_url = link_tag.get("href")

                if len(final_title) < 5:
                    continue

                if "宏福苑" not in final_title and "大火" not in final_title and "火災" not in final_title:
                    continue

                article_container = title_tag.find_parent("div")
                date_text = "Unknown Date"

                if article_container:
                    time_tag = article_container.find("time")
                    if time_tag:
                        date_text = time_tag.get_text(strip=True)
                    else:
                        container_text = article_container.get_text()
                        match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日)", container_text)
                        if match:
                            date_text = match.group(1)

                if link_url in all_articles:
                    if len(final_title) > len(all_articles[link_url]["title"]):
                        all_articles[link_url]["title"] = final_title
                        if date_text != "Unknown Date":
                            all_articles[link_url]["date"] = date_text
                else:
                    all_articles[link_url] = {"date": date_text, "title": final_title, "link": link_url}

            next_link = soup.find("a", class_=re.compile(r"next"))
            if not next_link:
                next_link = soup.find("a", string=re.compile(r"Next|下一頁|Older"))

            if not next_link:
                break

            page += 1
            time.sleep(1)

        except Exception:
            break

    results = []
    for link, article in all_articles.items():
        date_key = article["date"]
        date_match = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日)", date_key)
        if date_match:
            date_key = date_match.group(1)
        results.append((date_key, article["title"], link))

    return ("Points Media", results)
