# This is a news paper url scraper for Hong Kong Fire Documentary

A simple web scraper for scraping urls from sites

## Features

- **Multi-source scraping**: Scrapes articles from 19+ news sources including:
  - Local: TVB News, RTHK, HK01, Ming Pao, iCable, OnCC, Oriental Daily, Sky Post, The Sun
  - International: Guardian, CNN, Sky News
  - Government: HKGOV Press Releases
  - Other: Points Media, TVBS News, DotDotNews, People's Daily, HKEJ

- **Automatic deduplication**: Prevents duplicate articles from being added to the repository
- **Organized output**: Saves articles as markdown files in `content/news/` organized by news source
- **Date-based grouping**: Articles are grouped by publication date within each source directory
- **Incremental updates**: Appends new articles to existing files without overwriting

## Prerequisites

- **Python 3.13**
- **uv** (Python package manager and virtual environment tool)

## Installation & Usage

```bash
# Install dependencies and create virtual environment
uv sync

# Run the scraper
uv run main.py
```

`uv` will automatically create a virtual environment and download all required packages.

## How It Works

1. **Discovery**: Automatically discovers and imports all scraper modules from `./scrapers/`
2. **Execution**: Runs each scraper's `scrape()` function to fetch articles
3. **Processing**: Deduplicates articles based on URLs and existing content
4. **Storage**: Saves articles as markdown to corresponding directories in `content/news/`
5. **Formatting**: Groups articles by date within each source

## Adding New Scrapers

To add a new news source:

1. Create a new Python file in `./scrapers/` (e.g., `scrape_newssite.py`)
2. Implement a `scrape()` function that returns a tuple:

   ```python
   def scrape():
       # Your scraping logic here
       return source_title, content
   ```

   where `source_title` is a string (must match a key in `SOURCE_DIR_MAP` in `main.py`) and `content` is a list of tuples: `[(date, article_title, url), ...]`
3. Add a mapping entry in `main.py`'s `SOURCE_DIR_MAP` dictionary:

   ```python
   "News Site Name": "directory-name"
   ```

4. Create the corresponding directory in `content/news/` if it doesn't exist
5. Run the scraper

## Scraper Mapping

Available news sources are mapped to directories in `content/news/`. See `SOURCE_DIR_MAP` in `main.py` for the complete list of supported sources and their target directories.
