#!/usr/bin/env python3
import os
import csv
import re
import sys

def analyze_csv_file(file_path):
    errors = []
    html_warnings = []
    # Read the entire file into memory so we can make two passes
    try:
        with open(file_path, encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        errors.append(f"{os.path.basename(file_path)}, N/A, N/A, failed to open file: {e}")
        return errors, html_warnings

    # First pass: use csv.reader to check rows/fields anomalies.
    csv_reader = csv.reader(lines)
    try:
        header = next(csv_reader)
    except StopIteration:
        # Empty file, nothing to check
        return errors, html_warnings

    expected_fields = len(header)
    # Check header row anomalies
    for col_index, field in enumerate(header):
        # Check for utf-8 replacement char (�)
        pos = field.find("�")
        if pos != -1:
            errors.append(f"{os.path.basename(file_path)}, row 1, character position {pos+1}, utf-8 anomaly detected in header field {col_index}")
        # Check for carriage returns (\r) and line feeds (\n) in the field text
        pos = field.find("\r")
        if pos != -1:
            errors.append(f"{os.path.basename(file_path)}, row 1, character position {pos+1}, carriage return detected in header field {col_index}")
        pos = field.find("\n")
        if pos != -1:
            errors.append(f"{os.path.basename(file_path)}, row 1, character position {pos+1}, line feed detected in header field {col_index}")

    # Process subsequent rows
    field_count_mismatch = False
    row_number = 2
    for row in csv_reader:
        if len(row) != expected_fields:
            errors.append(f"{os.path.basename(file_path)}, row {row_number}, N/A, field count mismatch: expected {expected_fields} but got {len(row)}")
            field_count_mismatch = True
        # Check each field in the row for anomalies
        for col_index, field in enumerate(row):
            pos = field.find("�")
            if pos != -1:
                field_name = header[col_index] if col_index < len(header) else f"column {col_index}"
                errors.append(f"{os.path.basename(file_path)}, row {row_number}, character position {pos+1}, utf-8 anomaly detected in field {field_name}")
            pos = field.find("\r")
            if pos != -1:
                field_name = header[col_index] if col_index < len(header) else f"column {col_index}"
                errors.append(f"{os.path.basename(file_path)}, row {row_number}, character position {pos+1}, carriage return detected in field {field_name}")
            pos = field.find("\n")
            if pos != -1:
                field_name = header[col_index] if col_index < len(header) else f"column {col_index}"
                errors.append(f"{os.path.basename(file_path)}, row {row_number}, character position {pos+1}, line feed detected in field {field_name}")
        row_number += 1

    # Second pass: if all rows have the expected number of fields, check for HTML in each field.
    if not field_count_mismatch:
        # A simple regex to check for HTML tags
        html_pattern = re.compile(r'<[^>]+>')
        # We'll keep track of fields that have already been reported for HTML.
        reported_fields = set()
        # Use csv.DictReader so we can associate field content with header names.
        reader = csv.DictReader(lines)
        for row in reader:
            for field_name, field in row.items():
                if field and field_name not in reported_fields and html_pattern.search(field):
                    reported_fields.add(field_name)
        for field_name in reported_fields:
            html_warnings.append(f"{os.path.basename(file_path)}, {field_name}, html detected!!")

    return errors, html_warnings

def analyze_folder(folder_path):
    all_errors = []
    all_html_warnings = []
    # Loop through all files in the given folder
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            errors, html_warnings = analyze_csv_file(file_path)
            all_errors.extend(errors)
            all_html_warnings.extend(html_warnings)
    return all_errors, all_html_warnings

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python analyze_csv.py <folder_path>")
        sys.exit(1)
    folder_path = sys.argv[1]
    errors, html_warnings = analyze_folder(folder_path)
    # Print each error message with two newlines at the end.
    for msg in errors:
        print(msg + "\n")
    for msg in html_warnings:
        print(msg + "\n")
