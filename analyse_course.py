import csv
import os
import re
import sys

csv.field_size_limit(10000000)

# --- Argument parsing ---
if len(sys.argv) != 2:
    print("Usage: python analyse_course.py <course_folder>")
    print("Example: python analyse_course.py course_data/RVC_BVETMED3_2023_4")
    COURSE_PATH = "course_data/RVC_BVETMED3_2023_4"
else:
    COURSE_PATH = sys.argv[1]

if not os.path.isdir(COURSE_PATH):
    print(f"Error: Folder '{COURSE_PATH}' does not exist.")
    sys.exit(1)

COURSE_FOLDER = os.path.basename(COURSE_PATH)

# File paths
SECTIONS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_sections.csv')
MODULES_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_modules.csv')
LABELS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_labels.csv')
PAGES_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_pages.csv')
BOOKS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_books.csv')
FILES_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_files.csv')
FOLDERS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_folders.csv')
URLS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_urls.csv')
FORUMS_CSV = os.path.join(COURSE_PATH, f'{COURSE_FOLDER}_forums.csv')

OUTPUT_CSV = os.path.join(COURSE_PATH, 'course_analysis.csv')

# Helper: Read CSV as list of dicts
def read_csv_dict(filepath):
    with open(filepath, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

# 1. Section number → section name mapping (using 'section' column, not 'id')
sections = {}
try:
    for row in read_csv_dict(SECTIONS_CSV):
        # Map section number to section name
        section_num = str(row['section'])  # Convert to string for consistent mapping
        sections[section_num] = row['name']
        print(f"Section mapping: {section_num} -> {row['name']}")
except FileNotFoundError:
    print(f"Error: {SECTIONS_CSV} not found.")
    sys.exit(1)

# 2. Build module info: coursemodule id → {module_id, name, section_num}
modules = {}
try:
    with open(MODULES_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cmid = str(row['coursemodule'])  # Convert to string for consistent mapping
            modules[cmid] = {
                'module_id': row['id'],
                'name': row['name'],
                'section_num': str(row['section']),  # Use section number, convert to string
            }
            print(f"Module mapping: {cmid} -> section {row['section']}, name: {row['name'][:50]}...")
except FileNotFoundError:
    print(f"Error: {MODULES_CSV} not found.")
    sys.exit(1)

# 3. Build coursemodule id → module type mapping
module_type_map = {}
# Helper to map cmid from a type file
def map_type_cmid(type_csv, type_name, cmid_col):
    try:
        count = 0
        for row in read_csv_dict(type_csv):
            cmid = row.get(cmid_col)
            if cmid:
                module_type_map[str(cmid)] = type_name  # Convert to string
                count += 1
        print(f"Mapped {count} {type_name} modules")
    except FileNotFoundError:
        print(f"Warning: {type_csv} not found")

map_type_cmid(LABELS_CSV, 'label', 'label_cmid')
map_type_cmid(PAGES_CSV, 'page', 'page_cmid')
map_type_cmid(BOOKS_CSV, 'book', 'book_cmid')
map_type_cmid(FILES_CSV, 'file', 'file_cmid')
map_type_cmid(FOLDERS_CSV, 'folder', 'folder_cmid')
map_type_cmid(URLS_CSV, 'url', 'url_cmid')
map_type_cmid(FORUMS_CSV, 'forum', 'forum_cmid')

# 4. For labels: cmid → label content
label_content = {}
try:
    count = 0
    for row in read_csv_dict(LABELS_CSV):
        cmid = row.get('label_cmid')
        if cmid:
            label_content[str(cmid)] = row.get('content', '')  # Convert to string
            count += 1
    print(f"Loaded content for {count} labels")
except FileNotFoundError:
    print(f"Warning: {LABELS_CSV} not found")

def classify_label_content(content):
    if not content or content.strip() == '':
        return 'empty'
    # Check if content contains HTML tags
    if bool(re.search(r'<[^>]+>', content)):
        return 'html'
    return 'plain_text'

# 5. Write output CSV
print("\nGenerating analysis...")
with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['section_num', 'section_name', 'module_id', 'module_name', 'module_type', 'label_content_type'])
    
    for cmid, mod in modules.items():
        section_num = mod['section_num']
        section_name = sections.get(section_num, f'Unknown Section {section_num}')
        module_id = mod['module_id']
        module_name = mod['name']
        module_type = module_type_map.get(cmid, 'unknown')
        
        label_content_type = ''
        if module_type == 'label':
            content = label_content.get(cmid, '')
            label_content_type = classify_label_content(content)
        
        writer.writerow([section_num, section_name, module_id, module_name, module_type, label_content_type])

print(f'Analysis complete. Output written to {OUTPUT_CSV}')

# Print summary statistics
print("\n=== SUMMARY ===")
print(f"Total modules analyzed: {len(modules)}")
print(f"Sections found: {len(sections)}")
print(f"Module types found: {len(set(module_type_map.values()))}")

# Count modules by type
type_counts = {}
for module_type in module_type_map.values():
    type_counts[module_type] = type_counts.get(module_type, 0) + 1

print("\nModule type distribution:")
for module_type, count in sorted(type_counts.items()):
    print(f"  {module_type}: {count}")

# Count label content types
label_type_counts = {'empty': 0, 'plain_text': 0, 'html': 0}
for cmid in module_type_map:
    if module_type_map[cmid] == 'label':
        content = label_content.get(cmid, '')
        content_type = classify_label_content(content)
        label_type_counts[content_type] += 1

print("\nLabel content distribution:")
for content_type, count in label_type_counts.items():
    print(f"  {content_type}: {count}") 