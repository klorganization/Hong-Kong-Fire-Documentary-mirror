from googlesearch import search
from playwright.sync_api import sync_playwright


def scrape():
    results = []
    found_urls = set()

    query = 'site:news.sky.com "Hong Kong" fire'

    try:
        search_results = search(query, num_results=10, advanced=True)
        for result in search_results:
            url = result.url
            if "news.sky.com" in url:
                found_urls.add(url)
    except Exception:
        pass

    if len(found_urls) == 0:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            page = context.new_page()
            try:
                page.goto("https://news.sky.com/", timeout=60000)
                links_data = page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('a')).map(a => ({
                        text: a.innerText,
                        href: a.href
                    }))
                }""")

                for item in links_data:
                    title = item["text"].strip()
                    href = item["href"]
                    if not href:
                        continue

                    if "Hong Kong" in title or "Wang Fuk" in title or ("Fire" in title and "Hong Kong" in title):
                        if "/story/" in href or "/video/" in href:
                            found_urls.add(href)

            except Exception:
                pass
            finally:
                browser.close()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        for link in found_urls:
            page = context.new_page()
            try:
                page.goto(link, timeout=30000, wait_until="domcontentloaded")

                title = page.title().split("|")[0].strip()
                date_str = page.evaluate("""() => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (let s of scripts) {
                        try {
                            const data = JSON.parse(s.innerText);
                            if (data.datePublished) return data.datePublished;
                            if (data.uploadDate) return data.uploadDate;
                            if (Array.isArray(data)) {
                                for (let item of data) {
                                    if (item.datePublished) return item.datePublished;
                                    if (item.uploadDate) return item.uploadDate;
                                }
                            }
                        } catch(e) {}
                    }

                    const meta1 = document.querySelector('meta[name="pubdate"]');
                    if (meta1) return meta1.content;
                    const meta2 = document.querySelector('meta[property="article:published_time"]');
                    if (meta2) return meta2.content;

                    const timeEl = document.querySelector('time');
                    if (timeEl) return timeEl.getAttribute('datetime');

                    return null;
                }""")

                if not date_str:
                    date_str = "Unknown Date"

                results.append((date_str, title, link))

            except Exception:
                pass
            finally:
                page.close()

        browser.close()

    return ("Sky News", results)
