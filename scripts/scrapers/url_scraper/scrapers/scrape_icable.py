import requests
from bs4 import BeautifulSoup
import time
import re

def scrape():
    keyword = "宏福苑"
    search_url_template = "https://www.i-cable.com/page/{}/?s={}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    results = []
    page = 1
    has_more = True
    
    while has_more:
        url = search_url_template.format(page, keyword)
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 404:
                break
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.find_all('div', class_='cscra-blog-post')
            
            if not articles:
                break
                
            for article in articles:
                try:
                    title_tag = article.select_one('h4.post-title a')
                    if not title_tag:
                        continue
                        
                    title = title_tag.get_text(strip=True)
                    link = title_tag['href']
                    
                    date_text = "Unknown Date"
                    card_text = article.get_text(separator=' ', strip=True)
                    
                    date_match = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', card_text)
                    if date_match:
                        date_text = date_match.group(1)
                    
                    if "2025年11月" in date_text or "2025年12月" in date_text:
                        results.append((date_text, title, link))
                    
                except Exception:
                    pass
                    
            page += 1
            if page > 10: 
                break
            time.sleep(1) 
            
        except Exception:
            break
            
    return ("iCable", results)
