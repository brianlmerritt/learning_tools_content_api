#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import re

def extract_pluginfile_id(url):
    """
    Given a URL, extracts the numeric file ID that comes immediately after 'pluginfile.php/'.
    For example, from:
      https://learn.rvc.ac.uk/webservice/pluginfile.php/469138/mod_resource/...
    it will extract '469138'.
    """
    match = re.search(r'pluginfile\.php/(\d+)', url)
    if match:
        # Strip whitespace if any
        return match.group(1).strip()
    return None

def process_folder_for_files(folder_path):
    """
    For a given course folder, reads the course-specific files CSV and urls_found.csv, extracts pluginfile IDs
    from any URL in urls_found.csv that contains 'pluginfile.php/', and then writes two new CSV files:
      - files_used.csv: rows from the files CSV where the file_id matches one of the extracted IDs.
      - files_not_used.csv: rows from the files CSV where there is no match.
    """
    # Use the folder name (course idnumber) as the prefix for the files CSV
    course_id = os.path.basename(folder_path)
    files_csv_name = f"{course_id}_files.csv"
    files_csv_path = os.path.join(folder_path, files_csv_name)
    
    urls_found_csv_path = os.path.join(folder_path, "urls_found.csv")
    
    # Check that both necessary files exist.
    if not os.path.isfile(files_csv_path):
        print(f"Warning: {files_csv_path} not found. Skipping folder {folder_path}.")
        return
    if not os.path.isfile(urls_found_csv_path):
        print(f"Warning: {urls_found_csv_path} not found in folder {folder_path}. Skipping folder.")
        return
    
    try:
        df_files = pd.read_csv(files_csv_path, on_bad_lines='skip')
    except Exception as e:
        print(f"Error reading {files_csv_path}: {e}")
        return
    
    try:
        df_urls = pd.read_csv(urls_found_csv_path, on_bad_lines='skip')
    except Exception as e:
        print(f"Error reading {urls_found_csv_path}: {e}")
        return

    used_ids = set()
    # Ensure that the urls_found.csv contains the expected 'url' column.
    if 'url' not in df_urls.columns:
        print(f"Warning: 'url' column not found in {urls_found_csv_path}. Skipping folder.")
        return

    # Process each URL in the urls_found file that contains "pluginfile.php/".
    for url in df_urls['url']:
        if isinstance(url, str) and "pluginfile.php/" in url:
            file_id = extract_pluginfile_id(url)
            if file_id:
                used_ids.add(file_id)
    
    # Debug: print out the used IDs found
    print(f"Folder {folder_path}: Found pluginfile IDs from URLs: {used_ids}")

    # Convert the file_id column to string and strip whitespace for proper comparison.
    df_files['file_id'] = df_files['file_id'].astype(str).str.strip()

    # Create two DataFrames: one for files used (present in used_ids) and one for those not used.
    df_used = df_files[df_files['file_id'].isin(used_ids)]
    df_not_used = df_files[~df_files['file_id'].isin(used_ids)]
    
    # Write the results to new CSV files in the same folder.
    used_csv_path = os.path.join(folder_path, "files_used.csv")
    not_used_csv_path = os.path.join(folder_path, "files_not_used.csv")
    
    df_used.to_csv(used_csv_path, index=False)
    df_not_used.to_csv(not_used_csv_path, index=False)
    
    print(f"Folder {folder_path}: {len(df_used)} used files, {len(df_not_used)} not used files.")

def main():
    # Not ready until get all files works in get_moodle_courses_data.py
    raise NotImplementedError("This script is not ready until get all files works in get_moodle_courses_data.py")
    
    parser = argparse.ArgumentParser(
        description="Utility to check course files CSV against urls_found.csv and split files into used and not used based on pluginfile.php URLs."
    )
    parser.add_argument(
        "--idnumber",
        type=str,
        help="Process only the folder with this idnumber. If not provided, all folders in course_data are processed."
    )
    args = parser.parse_args()
    
    base_dir = os.path.join(os.getcwd(), "course_data")
    if not os.path.isdir(base_dir):
        print(f"Folder 'course_data' not found in {os.getcwd()}")
        return
    
    if args.idnumber:
        folder_path = os.path.join(base_dir, args.idnumber)
        if not os.path.isdir(folder_path):
            print(f"Folder for idnumber '{args.idnumber}' not found in {base_dir}.")
            return
        process_folder_for_files(folder_path)
    else:
        # Process all course folders
        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)
            if os.path.isdir(folder_path):
                process_folder_for_files(folder_path)

if __name__ == "__main__":
    main()
