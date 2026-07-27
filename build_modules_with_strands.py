#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_modules_with_strands.py

Builds {course}_modules_with_strands.csv for each BVetMed course folder.
Combines all content types (books, pages, labels, URLs, resources, folders, forums)
and labels them with strand information.

Strand assignment priority:
  1. Strand map links (parsed from the "Strand Map" page in the course)
  2. Section name (for items in sections that contain strand-mapped content)
  3. Content type fallback (for items not associated with any strand)

Usage:
    python build_modules_with_strands.py                       # all courses
    python build_modules_with_strands.py RVC_BVETMED1_2025_6   # one course

Layout - the script auto-detects course folders in this order:
    1. <script_dir>/course_data/   (preferred - script at project root)
    2. Any sibling subfolder containing course subdirectories
    3. <script_dir>/               (original layout - script alongside folders)
"""

import csv
import re
import os
import sys
import argparse
from html import unescape
from collections import defaultdict

csv.field_size_limit(10 ** 7)


def _find_base_dir():
    """Locate the directory that contains the course folders.

    Search order:
      1. A 'course_data' subdirectory next to this script
      2. Any subdirectory next to this script that itself contains subdirectories
      3. The script directory itself
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. Explicit 'course_data' subfolder
    candidate = os.path.join(script_dir, 'course_data')
    if os.path.isdir(candidate):
        return candidate

    # 2. Any sibling subfolder that contains subdirectories (i.e. is a data container)
    for entry in sorted(os.listdir(script_dir)):
        full = os.path.join(script_dir, entry)
        if os.path.isdir(full):
            if any(os.path.isdir(os.path.join(full, c)) for c in os.listdir(full)):
                return full

    # 3. Script directory itself
    return script_dir


BASE_DIR = _find_base_dir()

OUTPUT_COLUMNS = [
    'course_name', 'content_type', 'content_cmid', 'content_name',
    'section_id', 'section_name', 'strand', 'sub_strand',
    'week_number', 'week_date', 'in_strand_map',
]

COMBINED_OUTPUT_NAME = 'all_modules_with_strands.csv'


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def write_output_csv(path, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def normalize_cmid(cmid):
    """'185652.0' -> '185652', handle None/empty."""
    if not cmid:
        return ''
    cmid = str(cmid).strip()
    if cmid.endswith('.0'):
        cmid = cmid[:-2]
    return cmid


def clean_text(text):
    text = unescape(text or '')
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('\xa0', ' ')
    return re.sub(r'\s+', ' ', text).strip()


def list_course_folders(base_dir):
    """Return candidate course folders in a base directory."""
    return [
        name for name in sorted(os.listdir(base_dir))
        if os.path.isdir(os.path.join(base_dir, name))
        and not name.startswith(('.', '_'))
    ]


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def load_sections(folder_path, prefix):
    """Return (by_id, by_number) dicts of section records."""
    rows = load_csv(os.path.join(folder_path, f'{prefix}_sections.csv'))
    by_id, by_number = {}, {}
    for r in rows:
        name = unescape(r.get('name', '').strip())
        rec = {'id': r['id'], 'number': r['section'], 'name': name}
        by_id[r['id']] = rec
        by_number[r['section']] = rec
    return by_id, by_number


# ---------------------------------------------------------------------------
# Strand map detection and parsing
# ---------------------------------------------------------------------------

def find_strand_map_page(folder_path, prefix):
    """Return the strand-map page row (dict) or None."""
    rows = load_csv(os.path.join(folder_path, f'{prefix}_pages.csv'))
    candidates = [
        r for r in rows
        if 'strand map' in (r.get('page_name', '') or '').lower()
        and (r.get('clean_text', '') or '').strip()
    ]
    if not candidates:
        return None
    for c in candidates:
        if 'copy' not in (c.get('page_name', '') or '').lower():
            return c
    return candidates[0]


def parse_strand_legend(html):
    """{code: full_name} from the <li> legend in the strand map."""
    lis = re.findall(r'<li>(.*?)</li>', html, re.DOTALL)
    legend = {}
    for li in lis:
        text = clean_text(li)
        m = re.match(
            r'^([A-Z][A-Z0-9]*(?:\s*/\s*[A-Z][A-Z0-9]*)*)\s*[-\u2013\u2014]\s*(.+)$',
            text,
        )
        if m:
            codes_part, full_name = m.group(1), m.group(2).strip()
            full_name = re.sub(r'\s*\(collectively.*?\)', '', full_name).strip()
            full_name = re.sub(r'\s*\([A-Z]+ strand\)', '', full_name).strip()
            for code in (c.strip() for c in codes_part.split('/')):
                legend[code] = full_name
    return legend


def _match_text_to_strand_name(text, strand_names):
    """Try to match a text string to a known strand name.

    Checks exact, substring, and significant-word overlap.
    Returns the matched strand name or None.
    """
    if not text:
        return None
    tl = text.strip().lower()
    for name in sorted(strand_names, key=len, reverse=True):
        nl = name.lower()
        if tl == nl or nl in tl or tl in nl:
            return name
    for name in sorted(strand_names, key=len, reverse=True):
        nl = name.lower()
        words = {w for w in nl.split() if len(w) > 3}
        if words and sum(1 for w in words if w in tl) >= min(2, len(words)):
            return name
    return None


def _extract_sub_strand(rest):
    """Extract a clean sub-strand label from text after the strand code.

    Handles formats like:
      "(Tissue)"             -> "Tissue"
      "(Wk2) (Clin Path)"   -> "Clin Path"
      "(Small Mammal)"       -> "Small Mammal"
      "Animal Management"    -> "Animal Management"
    """
    if not rest:
        return None
    # Find all parenthesised groups
    groups = re.findall(r'\(([^)]+)\)', rest)
    if groups:
        # Filter out week markers like "Wk1", "Wk2", "WK7" and return the
        # last meaningful group (topic rather than week number)
        meaningful = [g.strip() for g in groups if not re.match(r'^Wk\d+$', g.strip(), re.IGNORECASE)]
        if meaningful:
            return meaningful[-1]
        # If only week markers, return the last one
        return groups[-1].strip()
    # No parentheses -> raw text
    return rest.strip() or None


def resolve_strand(link_text, legend, cmid, content_names,
                   cmid_to_section, all_strand_names=None):
    """(strand_full_name, sub_strand) for a single strand-map link."""
    text = (link_text or '').strip().replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text)
    if not text:
        return None, None

    _all = all_strand_names or set(legend.values())

    # Special case: Reflect and Review
    m = re.match(r'^(Reflect and Review)\s*(.*)$', text, re.IGNORECASE)
    if m:
        return 'Reflect and Review', m.group(2).strip() or None

    # Try legend codes, longest first
    for code in sorted(legend, key=len, reverse=True):
        if text.startswith(code):
            rest = text[len(code):]
            if rest and rest[0].isalpha():
                continue  # partial match of a longer token
            rest = rest.strip()
            sub = _extract_sub_strand(rest)
            return legend[code], sub

    # Try legend full-name values, longest first
    for name in sorted(_all, key=len, reverse=True):
        if text.lower().startswith(name.lower()):
            rest = text[len(name):].strip()
            sub = _extract_sub_strand(rest)
            return name, sub

    # Try matching the cmid's section name to a known strand
    sec_name = cmid_to_section.get(cmid)
    if sec_name:
        matched = _match_text_to_strand_name(sec_name, _all)
        if matched:
            return matched, None

    # Try matching the cmid's content name to a known strand
    cn = content_names.get(cmid)
    if cn:
        matched = _match_text_to_strand_name(cn, _all)
        if matched:
            return matched, None

    # Final fallbacks: section name, content name, or raw text
    if sec_name:
        return sec_name, None
    if cn:
        return cn, None
    return text, None


def parse_strand_table(html, legend, content_names, cmid_to_section,
                       all_strand_names=None):
    """Parse the week-by-week table.

    Returns list of dicts with keys:
        week_number, week_date, cmid, strand, sub_strand, link_text
    """
    trs = re.findall(r'<tr>(.*?)</tr>', html, re.DOTALL)
    entries = []
    week_num = 0

    for tr in trs:
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL)
        if len(tds) < 2:
            continue

        date_text = clean_text(tds[0])
        if not date_text or 'week beginning' in date_text.lower():
            continue

        content_td = tds[1]

        # Extract Moodle links (handle &amp; in URLs from HTML)
        links = re.findall(
            r'<a\s+href="(https://learn\.rvc\.ac\.uk/mod/(\w+)/view\.php\?id=(\d+)[^"]*)"[^>]*>([^<]+)</a>',
            content_td,
        )
        # Also handle course-level links (e.g. /course/view.php?id=...)
        course_links = re.findall(
            r'<a\s+href="(https://learn\.rvc\.ac\.uk/course/view\.php\?id=(\d+)[^"]*)"[^>]*>([^<]+)</a>',
            content_td,
        )

        if not links and not course_links:
            continue

        week_num += 1

        for url, mod_type, cmid, raw_text in links:
            link_text = unescape(raw_text).strip()
            strand, sub = resolve_strand(
                link_text, legend, cmid, content_names, cmid_to_section,
                all_strand_names,
            )
            entries.append({
                'week_number': week_num,
                'week_date': date_text,
                'cmid': cmid,
                'strand': strand,
                'sub_strand': sub,
                'link_text': link_text,
            })

        for url, course_id, raw_text in course_links:
            link_text = unescape(raw_text).strip()
            strand, sub = resolve_strand(
                link_text, legend, '', content_names, cmid_to_section,
                all_strand_names,
            )
            entries.append({
                'week_number': week_num,
                'week_date': date_text,
                'cmid': f'course_{course_id}',
                'strand': strand,
                'sub_strand': sub,
                'link_text': link_text,
            })

    return entries


# ---------------------------------------------------------------------------
# Content loading
# ---------------------------------------------------------------------------

def load_content_items(folder_path, prefix, sections_by_id, sections_by_number):
    """Load all content items from every CSV type.

    Returns (items, content_names, cmid_to_section)
        items           - list of dicts with standardised keys
        content_names   - {normalised_cmid: name}
        cmid_to_section - {normalised_cmid: section_name}
    """
    items = []
    content_names = {}
    cmid_to_section = {}

    def _lookup_section_by_id(raw_id):
        sid = normalize_cmid(raw_id)
        return sections_by_id.get(sid, {})

    # --- Books (deduplicate by book_cmid) ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_books.csv')):
        cmid = normalize_cmid(r.get('book_cmid', ''))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('book_section_id', ''))
        name = r.get('book_name', '')
        items.append({
            'content_type': 'book',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- Pages (deduplicate by page_cmid) ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_pages.csv')):
        cmid = normalize_cmid(r.get('page_cmid', ''))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('page_section_id', ''))
        name = r.get('page_name', '')
        items.append({
            'content_type': 'page',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- Labels ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_labels.csv')):
        cmid = normalize_cmid(r.get('label_cmid', ''))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('label_section_id', ''))
        name = r.get('label_name', '')
        items.append({
            'content_type': 'label',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- URLs ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_urls.csv')):
        cmid = normalize_cmid(r.get('url_cmid', ''))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('url_section_id', ''))
        name = r.get('url_name', '')
        items.append({
            'content_type': 'url',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- Resources (modules.csv) ---
    # Build section lookup from files.csv (has resource_section_id)
    file_section_map = {}
    for r in load_csv(os.path.join(folder_path, f'{prefix}_files.csv')):
        rc = normalize_cmid(r.get('resource_cmid', ''))
        if rc and rc not in file_section_map:
            file_section_map[rc] = r.get('resource_section_id', '')

    for r in load_csv(os.path.join(folder_path, f'{prefix}_modules.csv')):
        cmid = r.get('coursemodule', '').strip()
        if not cmid:
            continue
        # Try section from files.csv first (has section_id), else from modules section number
        raw_sec_id = file_section_map.get(cmid, '')
        if raw_sec_id:
            sec = _lookup_section_by_id(raw_sec_id)
        else:
            sec = sections_by_number.get(r.get('section', ''), {})
        name = r.get('name', '')
        items.append({
            'content_type': 'resource',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- Folders (deduplicate) ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_folders.csv')):
        cmid = normalize_cmid(r.get('folder_cmid', r.get('coursemodule', '')))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('folder_section_id', ''))
        name = r.get('folder_name', r.get('name', ''))
        items.append({
            'content_type': 'folder',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    # --- Forums (deduplicate) ---
    seen = set()
    for r in load_csv(os.path.join(folder_path, f'{prefix}_forums.csv')):
        cmid = normalize_cmid(r.get('forum_cmid', r.get('coursemodule', '')))
        if not cmid or cmid in seen:
            continue
        seen.add(cmid)
        sec = _lookup_section_by_id(r.get('forum_section_id', ''))
        name = r.get('forum_name', r.get('name', ''))
        items.append({
            'content_type': 'forum',
            'content_cmid': cmid,
            'content_name': name,
            'section_id': sec.get('id', ''),
            'section_name': sec.get('name', ''),
        })
        content_names[cmid] = name
        if sec.get('name'):
            cmid_to_section[cmid] = sec['name']

    return items, content_names, cmid_to_section


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process_course(folder_name, master_legend, write_folder_output=True):
    """Process one course folder and optionally write the per-course output CSV."""
    folder_path = os.path.join(BASE_DIR, folder_name)
    prefix = folder_name

    print(f'\nProcessing {folder_name} ...')

    sections_by_id, sections_by_number = load_sections(folder_path, prefix)
    items, content_names, cmid_to_section = load_content_items(
        folder_path, prefix, sections_by_id, sections_by_number,
    )
    print(f'  Loaded {len(items)} content items')

    strand_page = find_strand_map_page(folder_path, prefix)
    has_strand_map = strand_page is not None

    output_rows = []

    if has_strand_map:
        html = strand_page.get('cleanest_html') or strand_page.get('content') or ''
        local_legend = parse_strand_legend(html)
        combined_legend = {**master_legend, **local_legend}

        # Union of ALL known strand names (local + master) for matching
        all_strand_names = set(master_legend.values()) | set(local_legend.values())

        strand_entries = parse_strand_table(
            html, combined_legend, content_names, cmid_to_section,
            all_strand_names,
        )
        print(f'  Strand map: {len(strand_entries)} week-content entries')

        # Build cmid -> list of strand entries
        cmid_weeks = defaultdict(list)
        for e in strand_entries:
            cmid_weeks[e['cmid']].append(e)

        # Identify strand sections using two approaches:
        # 1. Sections containing strand-mapped items (with name-consistency check)
        # 2. Sections whose names match a legend entry directly
        strand_section_ids = set()
        section_strand_name = {}  # section_id -> authoritative strand name

        def _names_related(section_name, strand_name):
            """Check if a section name plausibly belongs to a strand."""
            sn = section_name.lower()
            st = strand_name.lower()
            if st in sn or sn in st:
                return True
            # Require at least 2 significant words (len > 3) to match
            s_words = {w for w in st.split() if len(w) > 3}
            if not s_words:
                return False
            matching = sum(1 for w in s_words if w in sn)
            return matching >= min(2, len(s_words))

        # Approach 1: strand-map items (only when strand was legend-derived)
        for item in items:
            cmid = item['content_cmid']
            if cmid in cmid_weeks and item['section_id']:
                strand_name = cmid_weeks[cmid][0]['strand']
                if strand_name in all_strand_names and _names_related(item['section_name'], strand_name):
                    strand_section_ids.add(item['section_id'])
                    if item['section_id'] not in section_strand_name:
                        section_strand_name[item['section_id']] = strand_name

        # Approach 2: match section names against ALL legend values
        for sec_id, sec_rec in sections_by_id.items():
            if sec_id in strand_section_ids:
                continue
            sn = sec_rec['name'].lower()
            if not sn.strip():
                continue
            for name in sorted(all_strand_names, key=len, reverse=True):
                if _names_related(sec_rec['name'], name):
                    strand_section_ids.add(sec_id)
                    section_strand_name[sec_id] = name
                    break

        for item in items:
            cmid = item['content_cmid']

            if cmid in cmid_weeks:
                # In strand map -> one row per week appearance
                for entry in cmid_weeks[cmid]:
                    output_rows.append({
                        'course_name': folder_name,
                        'content_type': item['content_type'],
                        'content_cmid': cmid,
                        'content_name': item['content_name'],
                        'section_id': item['section_id'],
                        'section_name': item['section_name'],
                        'strand': entry['strand'] or '',
                        'sub_strand': entry['sub_strand'] or '',
                        'week_number': entry['week_number'],
                        'week_date': entry['week_date'],
                        'in_strand_map': True,
                    })
            elif item['section_id'] in strand_section_ids:
                # Same section as strand content -> inherit section strand
                strand = section_strand_name.get(
                    item['section_id'], item['section_name']
                )
                output_rows.append({
                    'course_name': folder_name,
                    'content_type': item['content_type'],
                    'content_cmid': cmid,
                    'content_name': item['content_name'],
                    'section_id': item['section_id'],
                    'section_name': item['section_name'],
                    'strand': strand,
                    'sub_strand': '',
                    'week_number': '',
                    'week_date': '',
                    'in_strand_map': False,
                })
            else:
                # Not strand-related -> label by content type
                output_rows.append({
                    'course_name': folder_name,
                    'content_type': item['content_type'],
                    'content_cmid': cmid,
                    'content_name': item['content_name'],
                    'section_id': item['section_id'],
                    'section_name': item['section_name'],
                    'strand': item['content_type'],
                    'sub_strand': '',
                    'week_number': '',
                    'week_date': '',
                    'in_strand_map': False,
                })
    else:
        # No strand map -> everything labelled by content type
        for item in items:
            output_rows.append({
                'course_name': folder_name,
                'content_type': item['content_type'],
                'content_cmid': item['content_cmid'],
                'content_name': item['content_name'],
                'section_id': item['section_id'],
                'section_name': item['section_name'],
                'strand': item['content_type'],
                'sub_strand': '',
                'week_number': '',
                'week_date': '',
                'in_strand_map': False,
            })

    if write_folder_output:
        output_path = os.path.join(folder_path, f'{prefix}_modules_with_strands.csv')
        write_output_csv(output_path, output_rows)
        print(f'  Wrote {len(output_rows)} rows -> {os.path.basename(output_path)}')
    else:
        print(f'  Prepared {len(output_rows)} rows (per-folder output skipped)')

    return output_rows, (local_legend if has_strand_map else {})


def _matches_target(name, target):
    """Return True if folder name matches target (exact or wildcard with *)."""
    if target is None:
        return True
    import fnmatch
    return fnmatch.fnmatch(name, target) or name == target


def main():
    global BASE_DIR

    parser = argparse.ArgumentParser(
        description='Build per-course and combined modules_with_strands CSV outputs.',
    )
    parser.add_argument(
        'target',
        nargs='?',
        help='Optional course folder name or wildcard pattern to process.',
    )
    parser.add_argument(
        '--nofolders',
        action='store_true',
        help='Skip writing per-course modules_with_strands CSV files.',
    )
    args = parser.parse_args()

    target = args.target

    print(f'Looking for course folders in: {BASE_DIR}')

    # All course folders (any subdirectory not starting with . or _)
    all_folders = list_course_folders(BASE_DIR)

    # Folders to process
    folders = [f for f in all_folders if _matches_target(f, target)]

    if not folders:
        fallback_dir = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'course_data')
        )
        if os.path.abspath(BASE_DIR) != fallback_dir and os.path.isdir(fallback_dir):
            print(f'No matching folders in {BASE_DIR}; trying {fallback_dir}')
            fallback_all_folders = list_course_folders(fallback_dir)
            fallback_folders = [
                f for f in fallback_all_folders if _matches_target(f, target)
            ]
            if fallback_folders:
                BASE_DIR = fallback_dir
                all_folders = fallback_all_folders
                folders = fallback_folders
                print(f'Looking for course folders in: {BASE_DIR}')

    if not folders:
        print(f'No matching folders in {BASE_DIR}')
        sys.exit(1)

    # Pass 1: collect strand legends from ALL courses (even if processing one)
    master_legend = {}
    for folder in all_folders:
        page = find_strand_map_page(os.path.join(BASE_DIR, folder), folder)
        if page:
            html = page.get('cleanest_html') or ''
            legend = parse_strand_legend(html)
            master_legend.update(legend)

    print(f'Master legend ({len(master_legend)} codes):')
    for code in sorted(master_legend):
        print(f'  {code} -> {master_legend[code]}')

    # Pass 2: process each target course
    combined_rows = []
    for folder in folders:
        rows, local = process_course(
            folder,
            master_legend,
            write_folder_output=not args.nofolders,
        )
        combined_rows.extend(rows)
        master_legend.update(local)

    combined_output_path = os.path.join(BASE_DIR, COMBINED_OUTPUT_NAME)
    write_output_csv(combined_output_path, combined_rows)
    print(
        f'Wrote {len(combined_rows)} rows -> '
        f'{os.path.basename(combined_output_path)}'
    )

    print('\nDone.')


if __name__ == '__main__':
    main()
