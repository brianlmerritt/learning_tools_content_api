import pandas as pd
import numpy as np
import os
import dotenv

dotenv.load_dotenv()

from lib.moodle_rest import moodle_rest
from lib.moodle_content_helpers import moodle_content_helpers

idnumber_search = os.getenv('IDNUMBER_SEARCH')

moodle_rest_connection = moodle_rest()
moodle_content_helper = moodle_content_helpers(moodle_rest_connection)

courses = moodle_rest_connection.get_courses()


current_courses = moodle_rest_connection.get_matching_courses('idnumber', idnumber_search)

for _, current_course in current_courses.iterrows():
    course, course_modules, course_sections, course_blocks, course_resources = moodle_content_helper.set_course(current_course['id'])
    print(f"\n\nCurrent Course: {course['fullname']}")
    block_content, book_content, course_sections, course_file_content, course_resources = moodle_content_helper.get_course_content(course, course_modules, course_sections, course_blocks, course_resources)
    moodle_content_helper.save_course_data(course, course_sections, course_resources, block_content, book_content, course_file_content)

