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

# 1. Build section mappings using both section number and section ID
sections_by_num = {}  # section number -> name
sections_by_id = {}   # section ID -> name
try:
    for row in read_csv_dict(SECTIONS_CSV):
        section_num = str(row['section'])
        section_id = str(row['id'])
        section_name = row['name']
        sections_by_num[section_num] = section_name
        sections_by_id[section_id] = section_name
        print(f"Section mapping: num={section_num}, id={section_id} -> {section_name}")
except FileNotFoundError:
    print(f"Error: {SECTIONS_CSV} not found.")
    sys.exit(1)

# 2. Build regular module info from modules.csv
modules = {}
try:
    with open(MODULES_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cmid = str(row['coursemodule'])
            modules[cmid] = {
                'module_id': row['id'],
                'name': row['name'],
                'section_num': str(row['section']),
                'source': 'modules.csv'
            }
            print(f"Module: {cmid} -> section {row['section']}, name: {row['name'][:50]}...")
except FileNotFoundError:
    print(f"Error: {MODULES_CSV} not found.")
    sys.exit(1)

# 3. Add labels from labels.csv as they are separate modules
label_content = {}
try:
    with open(LABELS_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cmid = str(row['label_cmid'])
            section_id = str(row['label_section_id'])
            
            # Map section ID to section number for consistency
            section_num = None
            for s_num, s_id in [(n, sid) for n, sid in zip(sections_by_num.keys(), sections_by_id.keys()) if sections_by_id[sid] == sections_by_id.get(section_id, '')]:
                section_num = s_num
                break
            
            # If we can't find section number, use the section ID directly
            if section_num is None:
                # Find section number by matching section ID
                for s_num, s_name in sections_by_num.items():
                    if sections_by_id.get(section_id, '') == s_name:
                        section_num = s_num
                        break
                if section_num is None:
                    section_num = f"id_{section_id}"
            
            modules[cmid] = {
                'module_id': row['label_id'],
                'name': row['label_name'],
                'section_num': section_num,
                'source': 'labels.csv'
            }
            
            # Store label content for analysis
            content = row.get('content', '')
            label_content[cmid] = content
            
            print(f"Label: {cmid} -> section {section_num} ({section_id}), name: {row['label_name'][:50]}...")
            
except FileNotFoundError:
    print(f"Warning: {LABELS_CSV} not found")

# 4. Build coursemodule id → module type mapping
module_type_map = {}

# Helper to map cmid from a type file
def map_type_cmid(type_csv, type_name, cmid_col):
    try:
        count = 0
        for row in read_csv_dict(type_csv):
            cmid = row.get(cmid_col)
            if cmid:
                module_type_map[str(cmid)] = type_name
                count += 1
        print(f"Mapped {count} {type_name} modules")
    except FileNotFoundError:
        print(f"Warning: {type_csv} not found")

# Map all module types including labels
map_type_cmid(LABELS_CSV, 'label', 'label_cmid')
map_type_cmid(PAGES_CSV, 'page', 'page_cmid')
map_type_cmid(BOOKS_CSV, 'book', 'book_cmid')
map_type_cmid(FILES_CSV, 'file', 'file_cmid')
map_type_cmid(FOLDERS_CSV, 'folder', 'folder_cmid')
map_type_cmid(URLS_CSV, 'url', 'url_cmid')
map_type_cmid(FORUMS_CSV, 'forum', 'forum_cmid')

def classify_label_content(content):
    if not content or content.strip() == '':
        return 'empty'
    # Check if content contains HTML tags
    if bool(re.search(r'<[^>]+>', content)):
        return 'html'
    return 'plain_text'

# 5. Write output CSV
print(f"\nGenerating analysis for {len(modules)} total modules...")
with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['section_num', 'section_name', 'module_id', 'module_name', 'module_type', 'label_content_type', 'source'])
    
    for cmid, mod in modules.items():
        section_num = mod['section_num']
        section_name = sections_by_num.get(section_num, f'Unknown Section {section_num}')
        module_id = mod['module_id']
        module_name = mod['name']
        module_type = module_type_map.get(cmid, 'unknown')
        source = mod['source']
        
        label_content_type = ''
        if module_type == 'label':
            content = label_content.get(cmid, '')
            label_content_type = classify_label_content(content)
        
        writer.writerow([section_num, section_name, module_id, module_name, module_type, label_content_type, source])

print(f'Analysis complete. Output written to {OUTPUT_CSV}')

# Print summary statistics
print("\n=== SUMMARY ===")
print(f"Total modules analyzed: {len(modules)}")
print(f"  - From modules.csv: {len([m for m in modules.values() if m['source'] == 'modules.csv'])}")
print(f"  - From labels.csv: {len([m for m in modules.values() if m['source'] == 'labels.csv'])}")
print(f"Sections found: {len(sections_by_num)}")
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