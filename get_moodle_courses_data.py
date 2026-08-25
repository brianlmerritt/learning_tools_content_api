import argparse

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from lib import config
from lib.content_store import content_store
from lib.moodle_rest import moodle_rest
from lib.moodle_content_helpers import moodle_content_helpers

parser = argparse.ArgumentParser(description='Fetch configured courses into the sqlite content store.')
parser.add_argument('--force', action='store_true',
                    help='re-extract courses even when their contents hash is unchanged')
args = parser.parse_args()

# Course filters and environment selection come from config.yaml (credentials
# and server URLs stay in .env).
idnumber_search = config.idnumber_search()
idnumber_search_list = config.idnumber_search_list()
if idnumber_search not in [None, '']:
    idnumber_search_list.append(idnumber_search)
idnumber_list = config.idnumber_list()
use_uat = config.use_uat()

moodle_rest_connection = moodle_rest(use_uat=use_uat)
moodle_content_helper = moodle_content_helpers(moodle_rest_connection)
store = content_store(config.sqlite_path())

print(f"use_uat: {use_uat}")
print(f"Moodle base URL: {moodle_rest_connection.moodle_url}")
print(f"Content store: {config.sqlite_path()}")

current_courses = pd.DataFrame()

# Get lists courses first
if idnumber_list is not None and idnumber_list != ['']:
    matched_from_list = moodle_rest_connection.get_matching_courses_from_list('idnumber', idnumber_list)
    if matched_from_list is not None and not matched_from_list.empty:
        current_courses = pd.concat([current_courses, matched_from_list], ignore_index=True)

print(f"Found {len(current_courses)} courses from the course ids in idnumber_list provided.")

# Get search courses next (idnumber_search_list patterns plus legacy idnumber_search)
if idnumber_search_list:
    matched_from_search = moodle_rest_connection.get_matching_courses_from_list('idnumber', idnumber_search_list)
    if matched_from_search is not None and not matched_from_search.empty:
        current_courses = pd.concat([current_courses, matched_from_search], ignore_index=True)

if not current_courses.empty and 'id' in current_courses.columns:
    current_courses = current_courses.drop_duplicates(subset=['id']).reset_index(drop=True)

print(f"We now have a total of {len(current_courses)} courses including idnumbers and course ids.")

if current_courses.empty:
    print("No accessible courses matched the supplied idnumber filters.")

skipped = 0
for _, current_course in current_courses.iterrows():
    course, course_modules, course_sections, course_blocks, course_resources = moodle_content_helper.set_course(current_course['id'])
    contents_hash = moodle_content_helper.contents_hash()
    # Unchanged since the last run (same get_contents hash): skip the
    # expensive per-module content fetches. --force re-extracts regardless.
    if not args.force and store.unchanged(course['id'], contents_hash):
        skipped += 1
        print(f"Unchanged, skipped: {course['fullname']}")
        continue
    print(f"\n\nCurrent Course: {course['fullname']}")
    block_content, book_content, course_page_content, course_label_content, course_sections, course_file_content, course_folder_content, course_resources, course_urls, course_forums = moodle_content_helper.get_course_content(course, course_modules, course_sections, course_blocks, course_resources)
    moodle_content_helper.save_course_data(store, contents_hash, course, course_modules, course_sections, course_resources, block_content, book_content, course_file_content, course_folder_content, course_page_content, course_label_content, course_urls, course_forums)
    print(f"Saved data for {course['fullname']}")

print(f"\nDone. {skipped} unchanged course(s) skipped.")
