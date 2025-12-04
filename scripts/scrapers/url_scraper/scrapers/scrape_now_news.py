import requests 
from datetime import datetime

def scrape(): 
    base_url = 'https://newsapi1.now.com/pccw-news-api/api/getNewsListv2'
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    page_size = 100
    params = {
        'category': 119, # local news
        'pageSize': page_size,
        'pageNo': 1
    }

    target_tags = set(['宏福苑', '五級火',])
    def is_relevant(article): 
        tags = set([x['tag'] for x in article['newsTags']])
        context = article['title'] + ''.join([x['value'] for x in article['newsContent'] if x['newsType'] == 'text'])

        if tags & target_tags: 
            return True 
        if '宏福苑' in context: 
            return True 
        return False

    results = []
    has_next = True 
    while has_next: 
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        articles = response.json()
        for article in articles: 
            url = f'https://news.now.com/home/local/player?newsId={article["newsId"]}'
            title = article['title']
            date = datetime.fromtimestamp(article['publishDate'] / 1000).strftime('%F')

            if not is_relevant(article): 
                continue
            results.append((date, title, url))
        
        params['pageNo'] += 1
        has_next = (len(articles) == page_size)
    
    return ('NOW 新聞報導', results)
