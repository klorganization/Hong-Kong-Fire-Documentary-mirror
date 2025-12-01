import argparse
import json
import time

from waybackpy import WaybackMachineSaveAPI


def archive_url(url):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    try:
        save_api = WaybackMachineSaveAPI(url, user_agent)
        return save_api.save()
    except Exception as e:
        print(f"Error archiving {url}: {e}")
        return None


def process_database(db_path):
    print(f"Processing database: {db_path}")

    try:
        with open(db_path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Database file not found: {db_path}")
        return
    except json.JSONDecodeError:
        print(f"Invalid JSON in database file: {db_path}")
        return

    if "scraped_urls" not in data:
        print("Invalid database format: 'scraped_urls' key missing.")
        return

    modified = False
    urls = data["scraped_urls"]

    # Iterate over a copy of keys to allow modification if needed (though we modify values here)
    for url, info in urls.items():
        # Check if already archived
        if "archive_url" in info and info["archive_url"]:
            continue

        # Also skip if the original URL is already an archive link (unlikely but possible)
        if "web.archive.org" in url:
            continue

        print(f"Archiving {url}...")
        archive_link = archive_url(url)

        if archive_link:
            print(f"Archived to: {archive_link}")
            info["archive_url"] = str(archive_link)
            modified = True

            # Sleep to respect API rate limits (15 requests/min -> ~4s per request)
            time.sleep(5)
        else:
            print("Skipped or failed.")

    if modified:
        with open(db_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Updated database: {db_path}")
    else:
        print("No changes made to the database.")


def main():
    parser = argparse.ArgumentParser(description="Archive URLs in the JSON database to Wayback Machine.")
    parser.add_argument("db_path", help="Path to the scraped_urls.json file")
    args = parser.parse_args()

    process_database(args.db_path)


if __name__ == "__main__":
    main()
