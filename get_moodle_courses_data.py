import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import json

load_dotenv()

from lib.moodle_rest import moodle_rest
from lib.moodle_content_helpers import moodle_content_helpers

idnumber_search = os.getenv('IDNUMBER_SEARCH')
idnumber_list = json.loads(os.getenv("IDNUMBER_LIST", "[]"))  
idnumber_list = idnumber_list if isinstance(idnumber_list, list) else []

moodle_rest_connection = moodle_rest()
moodle_content_helper = moodle_content_helpers(moodle_rest_connection)

courses = moodle_rest_connection.get_courses()

current_courses = pd.DataFrame()

# Get lists courses first
if idnumber_list is not None and idnumber_list != ['']:
    current_courses = pd.concat([current_courses, moodle_rest_connection.get_matching_courses_from_list('idnumber', idnumber_list)], ignore_index=True)

# Get search courses next
if idnumber_search not in [None, '']:
    current_courses = pd.concat([current_courses, moodle_rest_connection.get_matching_courses('idnumber', idnumber_search)], ignore_index=True)

for _, current_course in current_courses.iterrows():
    course, course_modules, course_sections, course_blocks, course_resources = moodle_content_helper.set_course(current_course['id'])
    print(f"\n\nCurrent Course: {course['fullname']}")
    block_content, book_content, course_page_content, course_label_content, course_sections, course_file_content, course_folder_content, course_resources = moodle_content_helper.get_course_content(course, course_modules, course_sections, course_blocks, course_resources)
    moodle_content_helper.save_course_data(course, course_sections, course_resources, block_content, book_content, course_file_content, course_folder_content, course_page_content, course_label_content)
    print(f"Saved data for {course['fullname']}")

