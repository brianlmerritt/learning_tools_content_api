from typing import Dict, List, Tuple, Any
import pandas as pd
import os
import json
import csv
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
        self.data_store_path = 'course_data/'
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

    def save_course_data(self, course, course_sections, course_resources, course_blocks, course_books, course_files, course_folders, course_pages, course_labels, course_urls, course_forums):
        course_idnumber = course['idnumber']
        self.save_item_raw(course, course_idnumber, f"{course_idnumber}_course")
        self.save_item_raw(course_sections, course_idnumber, f"{course_idnumber}_sections")
        self.save_item_raw(course_resources, course_idnumber, f"{course_idnumber}_modules")
        self.save_item_raw(course_books, course_idnumber, f"{course_idnumber}_books")
        self.save_item_raw(course_blocks, course_idnumber, f"{course_idnumber}_blocks")
        self.save_item_raw(course_pages, course_idnumber, f"{course_idnumber}_pages")
        self.save_item_raw(course_labels, course_idnumber, f"{course_idnumber}_labels")
        self.save_item_raw(course_files, course_idnumber, f"{course_idnumber}_files")
        self.save_item_raw(course_folders, course_idnumber, f"{course_idnumber}_folders")
        self.save_item_raw(course_urls, course_idnumber, f"{course_idnumber}_urls")
        self.save_item_raw(course_forums, course_idnumber, f"{course_idnumber}_forums")
        return

    def save_item_raw(self, item_to_save, directory, filename):
        if isinstance(item_to_save, pd.DataFrame) :
            os.makedirs(os.path.dirname(f"{self.data_store_path}{directory}/{filename}.csv"), exist_ok=True)
            item_to_save.to_csv(f"{self.data_store_path}{directory}/{filename}.csv", index=False)
        elif isinstance(item_to_save, pd.Series):
            os.makedirs(os.path.dirname(f"{self.data_store_path}{directory}/{filename}.csv"), exist_ok=True)
            item_to_save.to_csv(f"{self.data_store_path}{directory}/{filename}.csv", header=False)
        elif isinstance(item_to_save, dict):
            with open(f"{self.data_store_path}{directory}/{filename}.json", "w") as file:
                json.dump(item_to_save, file)
        elif isinstance(item_to_save, list):
            with open(f"{self.data_store_path}{directory}/{filename}.csv", "w") as file:
                writer = csv.writer(file)
                writer.writerows(item_to_save)
        return
    
    def append_course_modules(self, course_modules, directory):
        full_directory_path = os.path.join(self.data_store_path, directory)
        os.makedirs(full_directory_path, exist_ok=True)  # Ensure the directory exists
        course_modules.to_csv(os.path.join(full_directory_path, "course_modules.csv"), mode='a', index=False)
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
        # Temp debug code
        self.append_course_modules(course_modules, "debug")

        course_block_content = self.block_content.get_block_content(course_blocks, course, course_resources)
        course_book_content = self.book_content.get_book_content(course_modules, course)
        course_page_content = self.page_content.get_page_content(course_modules, course)
        course_label_content = self.label_content.get_label_content(course_modules, course)
        course_file_content, course_folder_content = self.file_content.get_resource_content(course_modules, course)
        course_url_content = self.url_content.get_url_content(course_modules, course)
        course_forum_content = self.forum_content.get_forum_content(course_modules, course)
        return course_block_content, course_book_content, course_page_content, course_label_content, course_sections, course_file_content, course_folder_content, course_resources, course_url_content, course_forum_content

        # Lots more todo here
