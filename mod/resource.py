from typing import Dict, List, Tuple, Any
import pandas as pd
import httpx
import json
import csv
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
from lib.content_cleaners import content_cleaners
from lib.content_utilities import content_utilities


class mod_resource:
    def __init__(self, moodle_rest) -> None:
        self.content_cleaner = content_cleaners()
        self.content_utilities = content_utilities()
        self.moodle_rest = moodle_rest


    def get_context_from_url(self, url):
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('/')
        number_after_pluginfile = path_parts[path_parts.index('pluginfile.php') + 1]
        contextid    = int(number_after_pluginfile)
        return contextid

    def get_base_url(self, url):
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        return base_url

    def process_files_contents(self, course, resource):
        resource_list = []
        content = resource.get('contents', [])

        for file in content:
            file_dict = resource.copy()
            file_url = file.get('fileurl')
            base_url = self.get_base_url(file_url)

            file_dict['type'] = file.get('type')
            file_dict['filename'] = file.get('filename')
            file_dict['filepath'] = file.get('filepath')
            file_dict['filesize'] = file.get('filesize')
            file_dict['fileurl'] = file_url
            file_dict['baseurl'] = base_url
            file_dict['contextid'] = self.get_context_from_url(file_url)
            file_dict['timecreated'] = file.get('timecreated')
            file_dict['timemodified'] = file.get('timemodified')
            file_dict['sortorder'] = file.get('sortorder')
            file_dict['mimetypes'] = file.get('mimetypes')
            file_dict['isexternalfile'] = file.get('isexternalfile')
            file_dict['userid'] = file.get('userid')
            file_dict['author'] = file.get('author')
            file_dict['license'] = file.get('license')

            resource_list.append(file_dict)
        return resource_list

    # Process file contents
    def process_resources(self, files_or_folders, course):
        contents = []
        for _, file_or_folder in files_or_folders.iterrows():
            content_dict = {}
            content_dict['course_id'] = course.get('id')
            content_dict['course_name'] = course.get('fullname')
            content_dict['resource_name'] = file_or_folder.get('modname')
            content_dict['resource_plural'] = file_or_folder.get('modplural')
            content_dict['availability'] = file_or_folder.get('availability')
            content_dict['noviewlink'] = file_or_folder.get('noviewlink')
            content_dict['completion'] = file_or_folder.get('completion')
            content_dict['download'] = file_or_folder.get('download')
            content_dict['section_id'] = file_or_folder.get('section_id')
            content_dict['resource_url'] = file_or_folder.get('url')
            content_dict['contents'] = file_or_folder.get('contents')
            content_dict['contentsinfo'] = file_or_folder.get('contentsinfo')
            content_dict['availabilityinfo'] = file_or_folder.get('availabilityinfo')
            resource_list = self.process_files_contents(course, content_dict)
            contents.extend(resource_list)
        return contents
    

    def get_resource_content(self, course_resources, course):
        resource_contents = course_resources[(course_resources['modname'] == 'folder') | (course_resources['modname'] == 'resource')]
        resource_list = self.process_resources(resource_contents, course)
        return pd.DataFrame(resource_list)
