import argparse
import os
import re
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


def process_file(filepath):
    print(f"Processing {filepath}...")
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    # Regex for markdown links: [Title](URL)
    # We want to find links that are NOT followed by an Archive link
    # This is a simplified approach.
    # Improved regex to capture the whole link structure and check context

    # Pattern for table rows: | [Title](URL) |
    # We want to replace it with | [Title](URL) <br> ([Archive](ArchiveURL)) |

    # Pattern for list items: - [Title](URL)
    # We want to replace it with - [Title](URL) ([Archive](ArchiveURL))

    lines = content.splitlines()
    new_lines = []
    modified = False

    for line in lines:
        # Check for existing archive link to avoid double archiving
        if "web.archive.org" in line:
            new_lines.append(line)
            continue

        # Find all links in the line
        # This regex matches [Title](URL) but ignores internal links (starting with / or # or relative)
        matches = re.finditer(r"\[([^\]]+)\]\((http[^)]+)\)", line)

        new_line = line

        # We need to process matches in reverse order to not mess up indices if we were doing replacement by index
        # But here we are replacing strings.
        # Let's reconstruct the line.

        # Actually, simple string replacement might be risky if the same link appears twice.
        # Let's process distinct links.

        links_to_process = []
        for match in matches:
            url = match.group(2)

            # Skip if it's already an archive link (though the line check should catch this)
            if "web.archive.org" in url:
                continue

            links_to_process.append((match.group(0), url))

        for original_md, url in links_to_process:
            print(f"Archiving {url}...")
            archive_url_str = archive_url(url)
            if archive_url_str:
                print(f"Archived to: {archive_url_str}")

                # Determine format based on context (table or list)
                if "|" in line:
                    replacement = f"{original_md} <br> ([Archive]({archive_url_str}))"
                else:
                    replacement = f"{original_md} ([Archive]({archive_url_str}))"

                new_line = new_line.replace(original_md, replacement)
                modified = True
                # Sleep to be nice to the API (15 requests/min limit)
                time.sleep(5)
            else:
                print("Skipped or failed.")

        new_lines.append(new_line)

    if modified:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(new_lines) + "\n")
        print(f"Updated {filepath}")
    else:
        print(f"No changes for {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Archive links in markdown files to Wayback Machine.")
    parser.add_argument("path", help="File or directory to process")
    args = parser.parse_args()

    if os.path.isfile(args.path):
        process_file(args.path)
    elif os.path.isdir(args.path):
        for root, dirs, files in os.walk(args.path):
            for file in files:
                if file.endswith(".md"):
                    process_file(os.path.join(root, file))


if __name__ == "__main__":
    main()
