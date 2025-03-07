import os
import csv
import sys

def count_csv_rows_in_folders(base_path):
    if not os.path.isdir(base_path):
        print(f"Error: The path '{base_path}' is not a valid directory.")
        return
    
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        print(folder_path)
        
        if os.path.isdir(folder_path):  # Ensure it's a directory, not a file
            csv_file_path = os.path.join(folder_path, 'folders.csv')
            
            if os.path.isfile(csv_file_path):
                with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = sum(1 for row in reader) - 1  # Subtract 1 to exclude the header
                    
                    if rows > 0:
                        print(f"{folder}: {rows} rows")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path>")
    else:
        count_csv_rows_in_folders(sys.argv[1])