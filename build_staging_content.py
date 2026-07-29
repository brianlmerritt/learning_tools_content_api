#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_staging_content.py

Builds the two staging-ready CSVs consumed by the curriculum_mapping loader
refresh_curriculum_staging_learn_content:

    course_data/course_content.csv    - one row per course (cleaned summary)
    course_data/activity_content.csv  - one row per content chunk
                                        (page | label | url | book_chapter)

These load into curriculum_staging.moodle_course_content and
curriculum_staging.moodle_activity_content. Sources are the per-course folders
written by get_moodle_courses_data.py ({idnumber}_course.csv, _books.csv,
_pages.csv, _labels.csv, _urls.csv, _sections.csv). html_content = the
cleanest_html column, text_content = clean_text. Course summaries are cleaned
here with the same content_cleaners used for everything else.

Usage:
    python build_staging_content.py                       # all course folders
    python build_staging_content.py RVC_BVETMED1_2025_6   # one course
"""

import csv
import os
import sys
import argparse
from datetime import datetime, timezone
from html import unescape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.content_cleaners import content_cleaners

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


def find_base_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.join(script_dir, 'course_data')
    return candidate if os.path.isdir(candidate) else script_dir


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


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def load_course_info(folder_path, prefix):
    """{key: value} from the two-column {prefix}_course.csv (key,value rows)."""
    path = os.path.join(folder_path, f'{prefix}_course.csv')
    info = {}
    if not os.path.exists(path):
        return info
    with open(path, encoding='utf-8-sig') as f:
        for row in csv.reader(f):
            if len(row) >= 2:
                info[row[0].strip()] = row[1]
    return info


def load_section_names(folder_path, prefix):
    return {
        row.get('id', ''): clean_name(row.get('name', ''))
        for row in load_csv(os.path.join(folder_path, f'{prefix}_sections.csv'))
    }


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


def build_activity_rows(folder_path, folder_name, course_id, course_idnumber, sections):
    rows = []

    # Books: one row per html chapter ('file' rows are attachment components)
    for row in load_csv(os.path.join(folder_path, f'{folder_name}_books.csv')):
        if row.get('chapter_type') != 'html':
            continue
        rows.append(activity_row(
            course_id, course_idnumber, sections, 'book_chapter', row, 'book',
            item_id=to_int(row.get('chapter_id')),
            item_title=clean_name(row.get('chapter_title')),
            item_sortorder=to_int(row.get('chapter_sortorder')),
            activity_url=row.get('chapter_url', '') or row.get('book_url', '')))

    # Pages: one row per file component in the raw CSV - dedupe to page grain
    seen_pages = set()
    for row in load_csv(os.path.join(folder_path, f'{folder_name}_pages.csv')):
        page_id = row.get('page_id', '')
        if page_id in seen_pages:
            continue
        seen_pages.add(page_id)
        rows.append(activity_row(course_id, course_idnumber, sections, 'page', row, 'page'))

    for row in load_csv(os.path.join(folder_path, f'{folder_name}_labels.csv')):
        rows.append(activity_row(course_id, course_idnumber, sections, 'label', row, 'label'))

    for row in load_csv(os.path.join(folder_path, f'{folder_name}_urls.csv')):
        rows.append(activity_row(course_id, course_idnumber, sections, 'url', row, 'url'))

    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('course', nargs='?', help='single course folder to process')
    args = parser.parse_args()

    base_dir = find_base_dir()
    cleaner = content_cleaners()

    if args.course:
        folders = [args.course]
    else:
        folders = sorted(
            f for f in os.listdir(base_dir)
            if os.path.isdir(os.path.join(base_dir, f))
            and os.path.exists(os.path.join(base_dir, f, f'{f}_course.csv'))
        )

    course_rows = []
    activity_rows = []
    for folder_name in folders:
        folder_path = os.path.join(base_dir, folder_name)
        info = load_course_info(folder_path, folder_name)
        if not info.get('id'):
            print(f'SKIP {folder_name}: no usable {folder_name}_course.csv')
            continue
        course_row = build_course_row(info, folder_name, cleaner, folder_path)
        sections = load_section_names(folder_path, folder_name)
        acts = build_activity_rows(
            folder_path, folder_name,
            course_row['course_id'], course_row['course_idnumber'], sections)
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
