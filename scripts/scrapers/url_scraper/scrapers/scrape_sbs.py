import requests
from bs4 import BeautifulSoup
import json
import time

def scrape():
    collection_url = "https://www.sbs.com.au/news/tag/geography/hong-kong"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    results = []
    
    try:
        # Step 1: Get the list of articles
        print(f"Fetching collection: {collection_url}")
        response = requests.get(collection_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract article links
        # We look for links containing /news/article/
        # Use a set to avoid duplicates
        links_to_visit = []
        seen_urls = set()
        
        keywords = ["fire", "blaze", "Tai Po", "Wang Fuk"]

        for a in soup.find_all('a', href=True):
            href = a['href']
            # Include articles, podcasts, and videos
            if "/news/article/" in href or "/news/podcast-episode/" in href or "/news/video/" in href:
                # Ensure absolute URL
                if href.startswith("/"):
                    full_url = "https://www.sbs.com.au" + href
                else:
                    full_url = href
                
                # Basic check to avoid duplicates and ensure it's an article
                if full_url not in seen_urls:
                    title = a.get_text(strip=True)
                    if len(title) > 10: # Avoid small icons/buttons
                        # Filter by keywords
                        if any(k.lower() in title.lower() for k in keywords):
                            links_to_visit.append((title, full_url))
                            seen_urls.add(full_url)
        
        print(f"Found {len(links_to_visit)} articles. Processing top 10...")
        
        # Step 2: Visit each article to get the date
        for title, url in links_to_visit[:10]:
            try:
                # print(f"Visiting: {url}")
                article_resp = requests.get(url, headers=headers, timeout=10)
                if article_resp.status_code != 200:
                    continue
                    
                article_soup = BeautifulSoup(article_resp.text, 'html.parser')
                
                date_str = "Unknown"
                
                # Look for JSON-LD
                script_tag = article_soup.find('script', type='application/ld+json', attrs={'data-testid': 'product-jsonld'})
                if script_tag:
                    try:
                        data = json.loads(script_tag.string)
                        # The JSON-LD structure might be a dict or a list of dicts (graph)
                        if isinstance(data, dict) and '@graph' in data:
                            for item in data['@graph']:
                                if item.get('@type') == 'Article':
                                    date_str = item.get('datePublished', '')
                                    break
                        elif isinstance(data, dict) and data.get('@type') == 'Article':
                             date_str = data.get('datePublished', '')
                             
                        # Clean date: "2025-12-01T10:15:08.019Z" -> "2025-12-01"
                        if date_str and "T" in date_str:
                            date_str = date_str.split("T")[0]
                            
                    except json.JSONDecodeError:
                        pass
                
                # Fallback to meta tags if JSON-LD fails
                if date_str == "Unknown":
                    meta_date = article_soup.find('meta', attrs={'name': 'date'}) or \
                                article_soup.find('meta', property='article:published_time')
                    if meta_date:
                        date_str = meta_date.get('content', '')
                        if "T" in date_str:
                            date_str = date_str.split("T")[0]
                            
                results.append((date_str, title, url))
                time.sleep(0.5) # Be polite
                
            except Exception as e:
                print(f"Error processing article {url}: {e}")
                
    except Exception as e:
        print(f"Error scraping SBS: {e}")
        
    return ("SBS News (Australia)", results)

if __name__ == "__main__":
    name, res = scrape()
    print(f"Source: {name}")
    for date, title, link in res:
        print(f"[{date}] {title} ({link})")
