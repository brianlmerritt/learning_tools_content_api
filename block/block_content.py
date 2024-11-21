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

class block_content:
    def __init__(self) -> None:
        self.content_cleaner = content_cleaners()
        self.content_utilities = content_utilities()

    def get_block_content(self, course_blocks: pd.DataFrame, course_info: Dict[str, Any], course_resources: pd.DataFrame) -> pd.DataFrame:
        """
        Extract contents from block_text and combine with course information.
        
        Args:
            self: Class instance
            course_blocks: DataFrame containing block information
            course_info: Dictionary containing course metadata
            course_resources: DataFrame containing resource information
            
        Returns:
            DataFrame with block metadata and extracted contents
        """
        results = []
        
        # Create resource lookup dictionary
        resource_lookup = {}

        for _, resource in course_resources.iterrows():
            for file in resource.get('contentfiles', []):
                if resource_id := self.content_utilities.extract_resource_id(file.get('fileurl', '')):
                    resource_lookup[resource_id] = {
                        'resource_id': resource.get('id'),
                        'resource_coursemodule': resource.get('coursemodule'),
                        'resource_name': resource.get('name'),
                        'resource_visible': resource.get('visible'),
                        'resource_revision': resource.get('revision'),
                        'filename': file.get('filename'),
                        'filesize': file.get('filesize'),
                        'mimetype': file.get('mimetype'),
                        'fileurl': file.get('fileurl'),
                        'timemodified': file.get('timemodified')
                    }
        
        for _, block in course_blocks.iterrows():
            block_data = {
                'course_id': course_info.get('id'),
                'course_name': course_info.get('shortname'),
                'course_fullname': course_info.get('fullname'),
                'block_id': block.get('instanceid'),
                'block_name': block.get('block_title'),
                'block_type': block.get('name'),
                'visible': block.get('visible', False),
                'region': block.get('region'),
                'weight': block.get('weight'),
                'text_content': None,
                'url_content': [],
                'resources_content': []
            }

            if pd.notna(block.get('block_text')):
                soup = BeautifulSoup(block['block_text'], 'html.parser')
                
                block_data['text_content'] = self.content_cleaner.clean_text(soup.get_text(separator=' '))
                
                for a in soup.find_all('a'):
                    if href := a.get('href'):
                        url_data = {
                            'text': self.content_cleaner.clean_text(a.get_text()),
                            'url': self.content_cleaner.clean_url(href)
                        }
                        
                        # Check if URL is a pluginfile
                        if 'pluginfile.php' in href:
                            if resource_id := self.content_utilities.extract_resource_id(href):
                                if resource := resource_lookup.get(resource_id):
                                    url_data.update(resource)
                        
                        block_data['url_content'].append(url_data)
                
                for link in soup.find_all(['img', 'video', 'audio', 'source']):
                    src = link.get('src')
                    if src:
                        resource_data = {
                            'type': link.name,
                            'url': self.content_cleaner.clean_url(src),
                            'alt': self.content_cleaner.clean_text(link.get('alt', ''))
                        }
                        
                        if 'pluginfile.php' in src:
                            if resource_id := self.content_utilities.extract_resource_id(src):
                                if resource := resource_lookup.get(resource_id):
                                    resource_data.update(resource)
                        
                        block_data['resources_content'].append(resource_data)
            block_data = self.content_cleaner.clean_urls_in_dict(block_data)
            block_data = self.content_cleaner.clean_escaped_slashes(block_data)
            block_data = self.content_cleaner.clean_encoding_artifacts(block_data)
            results.append(block_data)
        
        return pd.DataFrame(results)