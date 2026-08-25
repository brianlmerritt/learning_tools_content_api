#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_staging_content.py

Builds the two staging-ready CSVs consumed by the curriculum_mapping loader
refresh_curriculum_staging_learn_content:

    course_data/course_content.csv    - one row per course (cleaned summary)
    course_data/activity_content.csv  - one row per content chunk
                                        (section | page | label | url |
                                         book_chapter)

These load into curriculum_staging.moodle_course_content and
curriculum_staging.moodle_activity_content. Source is the sqlite content
store (content.sqlite3 in course_data_dir, written by
get_moodle_courses_data.py; tables course, sections, books, pages, labels,
urls with the old CSV column names). html_content = the cleanest_html
column, text_content = clean_text. Course summaries are cleaned here with
the same content_cleaners used for everything else; cleaned assets land in
course_data_dir/<idnumber>/ as before.

Usage:
    python build_staging_content.py                       # every stored course
    python build_staging_content.py RVC_BVETMED1_2025_6   # one course
"""

import csv
import os
import sys
import argparse
from datetime import datetime, timezone
from html import unescape

from dotenv import load_dotenv

load_dotenv(override=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import config as runconfig
from lib.content_cleaners import content_cleaners
from lib.content_store import content_store

csv.field_size_limit(10 ** 7)

COURSE_COLUMNS = [
    'course_id', 'course_idnumber', 'shortname', 'fullname',
    'category_id', 'category_name', 'course_format', 'visible',
    'start_date', 'end_date', 'html_content', 'text_content',
]

ACTIVITY_COLUMNS = [
    'course_id', 'course_idnumber', 'activity_type', 'cmid', 'activity_name',
    'item_id', 'item_title', 'item_sortorder', 'section_id', 'section_name',
    'visible', 'activity_url', 'html_content', 'text_content', 'is_used',
]

COURSE_OUTPUT_NAME = 'course_content.csv'
ACTIVITY_OUTPUT_NAME = 'activity_content.csv'


def course_info(course_row):
    """Adapt a store manifest row to the old course-info key names."""
    info = dict(course_row)
    info['id'] = course_row.get('course_id')
    return info


def to_int(value):
    """'185399.0' -> 185399; ''/None/garbage -> ''."""
    if value in (None, ''):
        return ''
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return ''


def to_flag(value):
    """'1.0'/'True' -> 1; '0.0'/'False' -> 0; anything else -> ''."""
    if value in (None, ''):
        return ''
    v = str(value).strip().lower()
    if v in ('1', '1.0', 'true'):
        return 1
    if v in ('0', '0.0', 'false'):
        return 0
    return ''


def unix_to_utc(value):
    ts = to_int(value)
    if ts in ('', 0):
        return ''
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def clean_name(value):
    return unescape(value or '').strip()


def load_section_names(store, course_id):
    """{section_id_str: display name}, parent-prefixed for delegated rows.

    Delegated subsections (Moodle 4.5) carry parent_section_id. The parent
    week is the signal ("Monday" alone says nothing), so staged names carry
    it - "Week 1 - 6th Oct › Monday" - matching what local_curricmap
    displays and matches on.
    """
    rows = store.rows('sections', course_id)
    names = {
        str(to_int(row.get('id'))): clean_name(row.get('name') or '')
        for row in rows
    }
    prefixed = {}
    for row in rows:
        section_id = str(to_int(row.get('id')))
        name = names.get(section_id, '')
        parent = str(to_int(row.get('parent_section_id')))
        parent_name = names.get(parent, '') if parent else ''
        prefixed[section_id] = f'{parent_name} › {name}' if parent_name else name
    return prefixed


def active_moodle_url():
    """Return the Moodle base URL for the corpus being built (use_uat comes
    from config.yaml; the URLs themselves stay in .env with the credentials)."""
    setting = 'UAT_MOODLE_URL' if runconfig.use_uat() else 'MOODLE_URL'
    return os.getenv(setting, '').rstrip('/')


def build_course_row(info, folder_name, cleaner, folder_path):
    summary = cleaner.process_html_content(
        info.get('summary', ''), folder_path, 'course',
        info.get('id', ''), f'{folder_name}_summary', '0')
    return {
        'course_id': to_int(info.get('id')),
        'course_idnumber': info.get('idnumber', '') or folder_name,
        'shortname': clean_name(info.get('shortname')),
        'fullname': clean_name(info.get('fullname')),
        'category_id': to_int(info.get('categoryid')),
        'category_name': clean_name(info.get('categoryname')),
        'course_format': info.get('format', ''),
        'visible': to_flag(info.get('visible')),
        'start_date': unix_to_utc(info.get('startdate')),
        'end_date': unix_to_utc(info.get('enddate')),
        'html_content': summary['cleanest_html'],
        'text_content': summary['clean_text'],
    }


def activity_row(course_id, course_idnumber, sections, activity_type, row, prefix,
                 item_id='', item_title='', item_sortorder='', activity_url=''):
    section_id = row.get(f'{prefix}_section_id', '')
    return {
        'course_id': course_id,
        'course_idnumber': course_idnumber,
        'activity_type': activity_type,
        'cmid': to_int(row.get(f'{prefix}_cmid')),
        'activity_name': clean_name(row.get(f'{prefix}_name')),
        'item_id': item_id,
        'item_title': item_title,
        'item_sortorder': item_sortorder,
        'section_id': to_int(section_id),
        'section_name': sections.get(str(to_int(section_id)), ''),
        'visible': to_flag(row.get(f'{prefix}_visible')),
        'activity_url': activity_url or row.get(f'{prefix}_url', ''),
        'html_content': row.get('cleanest_html', ''),
        'text_content': row.get('clean_text', ''),
        'is_used': to_flag(row.get('is_used')),
    }


def build_section_rows(store, folder_path, folder_name, course_id, course_idnumber,
                       sections, cleaner, moodle_url):
    rows = []
    for row in store.rows('sections', course_id):
        section_id = to_int(row.get('id'))
        if section_id == '':
            continue
        # The sections map carries parent-prefixed names for delegated
        # subsections ("Week 1 - 6th Oct › Monday"); the bare name stays for
        # the cleaner's asset naming so file paths do not churn.
        bare_name = clean_name(row.get('name'))
        section_name = sections.get(str(section_id), '') or bare_name
        component_item_id = to_int(row.get('itemid'))
        summary = cleaner.process_html_content(
            row.get('summary', ''), folder_path, 'section', str(section_id),
            bare_name or f'section_{section_id}', str(component_item_id))
        rows.append({
            'course_id': course_id,
            'course_idnumber': course_idnumber,
            'activity_type': 'section',
            'cmid': '',
            'activity_name': section_name,
            'item_id': component_item_id,
            'item_title': clean_name(row.get('component')),
            'item_sortorder': to_int(row.get('section')),
            'section_id': section_id,
            'section_name': section_name,
            'visible': to_flag(row.get('visible')),
            'activity_url': (
                f'{moodle_url}/course/section.php?id={section_id}'
                if moodle_url else ''
            ),
            'html_content': summary['cleanest_html'],
            'text_content': summary['clean_text'],
            'is_used': '',
        })
    return rows


def build_activity_rows(store, folder_path, folder_name, course_id, course_idnumber,
                        sections, cleaner, moodle_url):
    rows = []

    rows.extend(build_section_rows(
        store, folder_path, folder_name, course_id, course_idnumber, sections,
        cleaner, moodle_url))

    # Books: one row per html chapter ('file' rows are attachment components)
    for row in store.rows('books', course_id):
        if row.get('chapter_type') != 'html':
            continue
        rows.append(activity_row(
            course_id, course_idnumber, sections, 'book_chapter', row, 'book',
            item_id=to_int(row.get('chapter_id')),
            item_title=clean_name(row.get('chapter_title')),
            item_sortorder=to_int(row.get('chapter_sortorder')),
            activity_url=row.get('chapter_url') or row.get('book_url') or ''))

    # Pages: one row per file component in the store - dedupe to page grain
    seen_pages = set()
    for row in store.rows('pages', course_id):
        page_id = row.get('page_id', '')
        if page_id in seen_pages:
            continue
        seen_pages.add(page_id)
        rows.append(activity_row(course_id, course_idnumber, sections, 'page', row, 'page'))

    for row in store.rows('labels', course_id):
        rows.append(activity_row(course_id, course_idnumber, sections, 'label', row, 'label'))

    for row in store.rows('urls', course_id):
        rows.append(activity_row(course_id, course_idnumber, sections, 'url', row, 'url'))

    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('course', nargs='?', help='single course folder to process')
    args = parser.parse_args()

    base_dir = runconfig.course_data_dir()
    store = content_store(runconfig.sqlite_path())
    cleaner = content_cleaners()
    moodle_url = active_moodle_url()

    stored = store.courses()
    if args.course:
        stored = [c for c in stored if (c.get('idnumber') or '') == args.course]
        if not stored:
            raise SystemExit(f'course not in the content store: {args.course}')

    course_rows = []
    activity_rows = []
    for manifest in stored:
        info = course_info(manifest)
        folder_name = info.get('idnumber') or f"course_{info['id']}"
        # Cleaned assets keep landing in a per-course directory as before.
        folder_path = os.path.join(base_dir, folder_name)
        os.makedirs(folder_path, exist_ok=True)
        course_id = info['id']
        course_row = build_course_row(info, folder_name, cleaner, folder_path)
        sections = load_section_names(store, course_id)
        acts = build_activity_rows(
            store, folder_path, folder_name,
            course_row['course_id'], course_row['course_idnumber'], sections,
            cleaner, moodle_url)
        course_rows.append(course_row)
        activity_rows.extend(acts)
        print(f'{folder_name}: {len(acts)} activity rows')

    for name, columns, rows in [
        (COURSE_OUTPUT_NAME, COURSE_COLUMNS, course_rows),
        (ACTIVITY_OUTPUT_NAME, ACTIVITY_COLUMNS, activity_rows),
    ]:
        path = os.path.join(base_dir, name)
        with open(path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        print(f'Wrote {len(rows)} rows -> {path}')


if __name__ == '__main__':
    main()
