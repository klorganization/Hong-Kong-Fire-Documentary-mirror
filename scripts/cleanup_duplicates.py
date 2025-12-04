#!/usr/bin/env python3
"""
Cleanup duplicate archive folders that end with -N suffix.
These are created when the scraper re-scrapes the same URL.

Safety: Only deletes if the base folder has the same URL.
"""

import json
import re
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
NEWS_DIR = PROJECT_ROOT / "content" / "news"

# Pattern to match folders ending with -1, -2, -3, etc.
DUPLICATE_PATTERN = re.compile(r"^(.+)-(\d+)$")


def get_url_from_metadata(folder: Path) -> str | None:
    """Extract URL from metadata.json"""
    metadata_file = folder / "metadata.json"
    if metadata_file.exists():
        try:
            with open(metadata_file, encoding="utf-8") as f:
                data = json.load(f)
                return data.get("url")
        except Exception:
            pass
    return None


def find_duplicates():
    """Find all duplicate folders and their base folders"""
    duplicates = []

    for source_dir in NEWS_DIR.iterdir():
        if not source_dir.is_dir():
            continue

        archive_dir = source_dir / "archive"
        if not archive_dir.exists():
            continue

        for folder in archive_dir.iterdir():
            if not folder.is_dir():
                continue

            match = DUPLICATE_PATTERN.match(folder.name)
            if match:
                base_name = match.group(1)
                suffix_num = int(match.group(2))
                base_folder = archive_dir / base_name

                duplicates.append(
                    {
                        "duplicate": folder,
                        "base_name": base_name,
                        "base_folder": base_folder,
                        "suffix": suffix_num,
                    }
                )

    return duplicates


def cleanup_duplicates(dry_run: bool = True):
    """Remove duplicate folders if they have the same URL as base"""
    duplicates = find_duplicates()

    print(f"Found {len(duplicates)} potential duplicate folders")

    removed = 0
    kept = 0
    renamed = 0
    errors = 0

    for dup in duplicates:
        duplicate_folder = dup["duplicate"]
        base_folder = dup["base_folder"]

        dup_url = get_url_from_metadata(duplicate_folder)

        if not dup_url:
            print(f"  SKIP (no URL): {duplicate_folder.relative_to(PROJECT_ROOT)}")
            errors += 1
            continue

        if base_folder.exists():
            base_url = get_url_from_metadata(base_folder)

            if base_url == dup_url:
                # Same URL - safe to delete duplicate
                print(f"  DELETE: {duplicate_folder.relative_to(PROJECT_ROOT)}")
                if not dry_run:
                    shutil.rmtree(duplicate_folder)
                removed += 1
            else:
                # Different URL - keep both
                print(f"  KEEP (different URL): {duplicate_folder.relative_to(PROJECT_ROOT)}")
                kept += 1
        else:
            # Base doesn't exist - rename duplicate to base
            print(f"  RENAME: {duplicate_folder.name} -> {base_folder.name}")
            if not dry_run:
                duplicate_folder.rename(base_folder)
            renamed += 1

    print("\nSummary:")
    print(f"  Removed: {removed}")
    print(f"  Renamed: {renamed}")
    print(f"  Kept (different URL): {kept}")
    print(f"  Errors: {errors}")

    if dry_run:
        print("\n*** DRY RUN - no changes made. Run with --execute to apply ***")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cleanup duplicate archive folders")
    parser.add_argument("--execute", action="store_true", help="Actually delete/rename (default is dry run)")
    args = parser.parse_args()

    cleanup_duplicates(dry_run=not args.execute)
