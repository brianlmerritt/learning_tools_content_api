from typing import Dict, List, Tuple, Any
import pandas as pd
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
from lib.content_cleaners import content_cleaners
from block.block_content import block_content
from mod.book import mod_book
from mod.page import mod_page
from mod.label import mod_label
from mod.resource import mod_resource
from mod.url import mod_url
from mod.forum import mod_forum

class moodle_content_helpers:
    def __init__(self, moodle_rest) -> None:
        # course_data_dir (config.yaml): e.g. course_data_uat for a uat fetch
        # so environments never share a corpus. Assets and legacy CSV helpers
        # use it; extracted content itself goes to the sqlite store.
        from lib.config import course_data_dir
        self.data_store_path = course_data_dir() + '/'
        self.moodle_rest = moodle_rest
        self.content_cleaner = content_cleaners()
        self.block_content = block_content()
        self.book_content = mod_book(moodle_rest)
        self.page_content = mod_page(moodle_rest)
        self.label_content = mod_label(moodle_rest)
        self.file_content = mod_resource(moodle_rest)
        self.url_content = mod_url(moodle_rest)
        self.forum_content = mod_forum(moodle_rest)

    def get_page_content(self, page_cmid):
        pass

    def get_forum_content(self, forum_cmid):
        pass

    def set_course(self, course_id):
        
        self.moodle_rest.set_course(course_id)
        course = self.moodle_rest.get_course(course_id)
        course_name = course['fullname']
        course_idnumber = course['idnumber']
        course_sections = self.moodle_rest.get_course_sections(course_id)
        course_modules = self.moodle_rest.get_course_modules(course_id)
        course_blocks = self.moodle_rest.get_course_blocks(course_id)
        course_resources = self.moodle_rest.get_course_resources(course_id)
        return course, course_modules, course_sections, course_blocks, course_resources

    def contents_hash(self):
        """Hash of the current course's core_course_get_contents payload -
        names, structure, visibility and the file timestamps inside module
        contents - the store's cheap has-anything-changed signal."""
        import hashlib
        payload = self.moodle_rest.current_course_content
        if not isinstance(payload, pd.DataFrame) or payload.empty:
            return ''
        return hashlib.sha1(payload.to_json().encode('utf-8')).hexdigest()

    def save_course_data(self, store, contents_hash, course, course_modules, course_sections, course_resources, course_blocks, course_books, course_files, course_folders, course_pages, course_labels, course_urls, course_forums):
        """Replace this course's rows in the sqlite content store (one
        transaction; ruled 2026-08-25, superseding the per-course CSV tree).
        Table names / columns mirror the old CSV files, plus course_modules =
        the true all-modules union from get_contents (subsection cms
        included), which the CSV era discarded. The 'modules' table keeps its
        historical (misleading) name from *_modules.csv: it is the
        mod_resource_get_resources_by_courses payload."""
        store.replace_course(course, contents_hash, {
            'sections': course_sections,
            'course_modules': course_modules,
            'modules': course_resources,
            'books': course_books,
            'blocks': course_blocks,
            'pages': course_pages,
            'labels': course_labels,
            'files': course_files,
            'folders': course_folders,
            'urls': course_urls,
            'forums': course_forums,
        })
        return

    def extract_file_details(self, content, all_files):
        file_details = [] # Todo make this work, add mod type perhaps
        return file_details

    def get_all_files(self, block_content, book_content, course_page_content, course_label_content, course_sections, course_file_content, course_folder_content, course_resources, course_urls, course_forums):
        all_files = course_file_content
        print(all_files.head()) # Todo add label, book, page content to files
        print(course_label_content.head())
        print(course_page_content.head())
        print(book_content.head())
        print(block_content.head())
        print(course_sections.head())
        print(course_resources.head())
        print(course_urls.head())
        print(course_forums.head())
        return all_files

    
    def get_course_content(self, course, course_modules, course_sections, course_blocks, course_resources):
        if not isinstance(course_modules, pd.DataFrame):
            course_modules = pd.DataFrame(columns=['modname'])
        elif 'modname' not in course_modules.columns:
            course_modules = course_modules.copy()
            course_modules['modname'] = pd.Series(dtype='object')

        if not isinstance(course_resources, pd.DataFrame):
            course_resources = pd.DataFrame(columns=['modname'])
        elif 'modname' not in course_resources.columns:
            course_resources = course_resources.copy()
            course_resources['modname'] = pd.Series(dtype='object')

        course_block_content = self.block_content.get_block_content(course_blocks, course, course_resources)
        course_book_content = self.book_content.get_book_content(course_modules, course)
        course_page_content = self.page_content.get_page_content(course_modules, course)
        course_label_content = self.label_content.get_label_content(course_modules, course)
        course_file_content, course_folder_content = self.file_content.get_resource_content(course_modules, course)
        course_url_content = self.url_content.get_url_content(course_modules, course)
        course_forum_content = self.forum_content.get_forum_content(course_modules, course)
        return course_block_content, course_book_content, course_page_content, course_label_content, course_sections, course_file_content, course_folder_content, course_resources, course_url_content, course_forum_content

        # Lots more todo here
