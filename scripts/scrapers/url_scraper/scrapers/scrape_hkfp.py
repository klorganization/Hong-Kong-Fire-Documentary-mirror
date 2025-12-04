import requests
from datetime import datetime
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


def scrape(): 
    url = 'https://hongkongfp.com/feed/'
    results = []

    def is_relevant(title, description, content, categories): 
        if 'Wang Fuk Court' in categories: 
            return True 
        
        context = (title + description + content).lower()
        if 'wang fuk' in context: 
            return True 
        if 'hong kong' in context and ('blaze' in context or 'fire' in context): 
            return True 
        return False

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    articles = soup.select('rss channel item')
    

    for article in articles: 
        title = article.find('title').text
        description = article.find('description').text
        content = article.find('content:encoded').text
        categories = [x.text for x in article.find_all('category')]

        if not is_relevant(title, description, content, categories): 
            continue 

        date_str = datetime.strptime(
            article.find('pubdate').text, 
            '%a, %d %b %Y %H:%M:%S %z'
        ).strftime('%F')
        link = article.find('guid').text

        results.append((date_str, title, link))

    return ('Hong Kong Free Press', results)
