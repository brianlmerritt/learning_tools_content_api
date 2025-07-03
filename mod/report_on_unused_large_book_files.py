import pandas as pd
import os
import urllib.parse
from pathlib import Path

def report_unused_moodle_book_files(base_path="course_data"):
    """
    Generate a report of the top 10 largest unused book files for each RVC course.
    
    Args:
        base_path (str): Path to the directory containing course folders
    
    Returns:
        pd.DataFrame: Report with course and file information
    """
    results = []
    
    # Get all RVC folders
    base_dir = Path(base_path)
    if not base_dir.exists():
        print(f"Directory {base_path} does not exist")
        return pd.DataFrame()
    
    rvc_folders = [folder for folder in base_dir.iterdir() 
                   if folder.is_dir() and folder.name.startswith("RVC")]
    
    print(f"Found {len(rvc_folders)} RVC course folders")
    
    for folder in rvc_folders:
        folder_name = folder.name
        print(f"Processing folder: {folder_name}")
        
        # Read course information
        course_file = folder / f"{folder_name}_course.csv"
        book_file = folder / f"{folder_name}_books.csv"
        
        if not course_file.exists():
            print(f"  Warning: Course file not found: {course_file}")
            continue
            
        if not book_file.exists():
            print(f"  Warning: Book file not found: {book_file}")
            continue
        
        try:
            # Read course data - it appears to be in a transposed format
            course_data = pd.read_csv(course_file, header=None, index_col=0)
            
            if course_data.empty:
                print(f"  Warning: Empty course file for {folder_name}")
                continue
            
            # Extract id and fullname from the transposed data
            course_id = None
            course_fullname = None
            
            if 'id' in course_data.index:
                course_id = course_data.loc['id', 1]  # Get value from column 1
            if 'fullname' in course_data.index:
                course_fullname = course_data.loc['fullname', 1]  # Get value from column 1
            
            if course_id is None or course_fullname is None:
                print(f"  Warning: Missing course id or fullname in {folder_name}")
                print(f"  Available fields: {course_data.index.tolist()}")
                continue
            
            print(f"  Course ID: {course_id}, Course Name: {course_fullname}")
            
            # Read book data
            book_data = pd.read_csv(book_file)
            if book_data.empty:
                print(f"  No book data for {folder_name}")
                continue
            
            # Create a standardized boolean column for filtering
            # Handle all possible representations of True/False
            def is_truthy(value):
                """Convert various representations to boolean"""
                if pd.isna(value):
                    return False
                str_val = str(value).lower().strip()
                return str_val in ['1', 'true', 'yes', 'y']
            
            # Create keep_filter column - True means keep (file is unused)
            book_data['keep_filter'] = ~book_data['is_used'].apply(is_truthy)
            
            # Filter unused files of type "file"
            filtered_books = book_data[
                (book_data['keep_filter'] == True) & 
                (book_data['chapter_type'] == 'file')
            ].copy()
            
            if filtered_books.empty:
                print(f"  No unused book files found for {folder_name}")
                continue
            
            # Convert filesize to numeric, handling any conversion issues
            filtered_books['chapter_filesize'] = pd.to_numeric(
                filtered_books['chapter_filesize'], errors='coerce'
            )
            
            # Remove rows where filesize couldn't be converted
            filtered_books = filtered_books.dropna(subset=['chapter_filesize'])
            
            if filtered_books.empty:
                print(f"  No valid filesizes found for {folder_name}")
                continue
            
            # Filter files >= 1MB (1,048,576 bytes) and sort by filesize descending
            large_files = filtered_books[
                filtered_books['chapter_filesize'] >= 1048576
            ].sort_values('chapter_filesize', ascending=False)
            
            if large_files.empty:
                print(f"  No unused files >= 1MB found for {folder_name}")
                continue
            
            # Process each file
            for _, row in large_files.iterrows():
                # URL decode the filename and add quotes
                filename = urllib.parse.unquote(row['chapter_filename'])
                filename_quoted = f'"{filename}"'
                
                results.append({
                    'course_id': course_id,
                    'course_fullname': course_fullname,
                    'course_idnumber': folder_name,  # FOLDERNAME_IDNUMBER
                    'book_name': row['book_name'],
                    'chapter_title': row['chapter_title'],
                    'filename': filename_quoted,
                    'filesize': int(row['chapter_filesize']),
                    'book_id': row['book_id'],
                    'book_cmid': row['book_cmid'],
                    'chapter_id': row['chapter_id']
                })
            
            print(f"  Found {len(large_files)} unused files >= 1MB for {folder_name}")
            
        except Exception as e:
            print(f"  Error processing {folder_name}: {str(e)}")
            continue
    
    # Create final report DataFrame
    if results:
        report_df = pd.DataFrame(results)
        print(f"\nTotal unused files found: {len(report_df)}")
        return report_df
    else:
        print("No unused files found across all courses")
        return pd.DataFrame()

def save_report_to_csv(report_df, output_file="unused_moodle_book_files_report.csv"):
    """
    Save the report to a CSV file.
    
    Args:
        report_df (pd.DataFrame): The report dataframe
        output_file (str): Output filename
    """
    if not report_df.empty:
        report_df.to_csv(output_file, index=False)
        print(f"Report saved to: {output_file}")
    else:
        print("No data to save")

# Usage example
if __name__ == "__main__":
    # Generate the report
    report = report_unused_moodle_book_files()
    
    # Save to CSV
    if not report.empty:
        save_report_to_csv(report, "unused_files.csv")
        
        # Display summary
        print("\nReport Summary:")
        print(f"Courses processed: {report['course_idnumber'].nunique()}")
        print(f"Total unused files: {len(report)}")
        print(f"Total file size: {report['filesize'].sum():,} bytes")
        
        # Show first few rows
        print("\nFirst 5 rows of report:")
        print(report.head().to_string(index=False))
    else:
        print("No unused book files found to report")