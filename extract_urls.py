#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import re

def extract_urls(text):
    """Extracts all http/https URLs from a given text using a regex."""
    if pd.isna(text):
        return []
    pattern = r'(https?://[^\s\'"<>]+)'
    raw_urls = re.findall(pattern, str(text))
    # Remove any leading/trailing double quotes from each URL
    cleaned_urls = [url.strip('"') for url in raw_urls]
    return cleaned_urls


def process_course_folder(course_folder):
    """
    Processes a single course folder by reading its CSV files (excluding certain ones),
    extracting URLs from columns whose names contain 'content' or 'html', and writing
    unique (filename, url) pairs to urls_found.csv in the folder.
    """
    # Files to ignore (based on suffix)
    ignore_suffixes = ("_course.csv", "_modules.csv", "_sections.csv", "_folders.csv", "_found.csv")
    # Dictionary to collect URLs per CSV filename
    urls_per_file = {}

    for filename in os.listdir(course_folder):
        if filename.endswith(".csv") and not filename.endswith(ignore_suffixes):
            file_path = os.path.join(course_folder, filename)
            try:
                df = pd.read_csv(file_path, on_bad_lines='skip')
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

            # Loop over columns with "content" or "html" in their names (case-insensitive)
            for col in df.columns:
                if "content" in col.lower() or "html" in col.lower() or "url" in col.lower():
                    for cell in df[col]:
                        for url in extract_urls(cell):
                            urls_per_file.setdefault(filename, set()).add(url)

    # Prepare data for output: each row is (filename, url)
    rows = []
    for fname, url_set in urls_per_file.items():
        for url in url_set:
            rows.append({"filename": fname, "url": url})

    # Write the output CSV file in the course folder
    output_path = os.path.join(course_folder, "urls_found.csv")
    output_df = pd.DataFrame(rows)
    output_df.to_csv(output_path, index=False)
    print(f"Processed {course_folder}: {len(rows)} unique URL(s) written to {output_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Extract URLs from course CSV files in course_data."
    )
    parser.add_argument(
        "--idnumber",
        type=str,
        help="Process only the folder with this idnumber. If not provided, all folders are processed."
    )
    args = parser.parse_args()

    # Assumes that course_data is a folder in the project root (current working directory)
    base_dir = os.path.join(os.getcwd(), "course_data")
    if not os.path.isdir(base_dir):
        print(f"Folder 'course_data' not found in {os.getcwd()}")
        return

    if args.idnumber:
        # Process only the specified course folder
        course_folder = os.path.join(base_dir, args.idnumber)
        if not os.path.isdir(course_folder):
            print(f"Folder for idnumber '{args.idnumber}' not found in {base_dir}.")
            return
        process_course_folder(course_folder)
    else:
        # Process all course folders in course_data
        for folder in os.listdir(base_dir):
            course_folder = os.path.join(base_dir, folder)
            if os.path.isdir(course_folder):
                process_course_folder(course_folder)

if __name__ == "__main__":
    main()
