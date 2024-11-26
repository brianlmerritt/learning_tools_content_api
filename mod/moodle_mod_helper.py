from typing import Dict, List, Any
import pandas as pd
import json
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from lib.content_cleaners import content_cleaners
from lib.content_utilities import content_utilities
from lib.event_logger import EventLogger

class ModuleHelper:
    """Helper class for processing Moodle module content"""
    
    def __init__(self, moodle_rest, modtype: str, component_name: str, content_field: str,
                       has_subcomponents: bool = False) -> None:
        """
        Initialize helper with module type and component name
        Args:
            moodle_rest: Moodle REST client
            modtype: Type of module (e.g., 'book', 'page', 'forum')
            component_name: Name of sub-components (e.g., 'chapter', 'post', 'entry')
        """
        self.content_cleaner = content_cleaners()
        self.content_utilities = content_utilities()
        self.moodle_rest = moodle_rest
        self.event_logger = EventLogger()
        self.modtype = modtype
        self.component_name = component_name
        self.content_field = content_field
        self.has_subcomponents = has_subcomponents
        self.data_store_path = 'course_data/'

    def process_mod_items(self, contents: List[dict], module_data: dict, course: dict) -> List[dict]:
        """Generic processor for module items"""
        items = []
        
        for content in contents:
            if content['type'] != 'file':
                self.event_logger.log_data(f'Unknown {self.modtype} {self.component_name} type', 
                    f"Content type: {content['type']} fileurl: {content['fileurl']} content: {content}")
                continue
            
            item = self._process_item(content, module_data, course)
            if item:
                items.append(item)
        
        items.sort(key=lambda x: x.get('sortorder', 0))
        return self._process_item_usage(items, module_data)

    def get_mod_content(self, course_modules: pd.DataFrame, course: dict) -> pd.DataFrame:
        """Generic getter for module content"""
        results = []

        for _, module_contents in course_modules.iterrows():
            module_data = self._create_base_module_data(module_contents, course)
            contents = module_contents.get(self.content_field, [])
            
            # Handle table of contents if present (e.g., for books)
            toc = []
            if self.modtype == 'book': # First item in book is table of contents
                if contents and contents[0].get('type') == 'content':
                    try:
                        toc = json.loads(contents[0]['content'])
                        contents = contents[1:]  # Skip TOC in processing
                    except:
                        pass
                        
            if toc:
                module_data['toc'] = toc

            if self.has_subcomponents:
                processed_items = self.process_mod_items(contents, module_data, course)
                results.extend(processed_items)
            else:
                output_path = os.path.join(self.data_store_path, course.get('idnumber'))
                processed_content = self.content_cleaner.process_html_content(
                    contents, 
                    output_path,
                    self.modtype,
                    str(module_data.get(f'{self.modtype}_cmid')),
                    module_data.get(f'{self.modtype}_name', ''),
                    module_data.get(f'{self.modtype}_instance', ''),
                )
                module_data.update(processed_content)
                results.append(module_data)

        return pd.DataFrame(results)

    def _create_base_module_data(self, module_contents: dict, course: dict) -> dict:
        """Create base module data dictionary"""
        return {
            'course_id': course.get('id'),
            'course_name': course.get('fullname'),
            f'{self.modtype}_id': module_contents.get('instance'),
            f'{self.modtype}_cmid': module_contents.get('id'),
            f'{self.modtype}_name': module_contents.get('name'),
            f'{self.modtype}_description': module_contents.get('description'),
            f'{self.modtype}_contextid': module_contents.get('contextid'),
            f'{self.modtype}_visible': module_contents.get('visible'),
            f'{self.modtype}_url': module_contents.get('url'),
            f'{self.modtype}_section_id': module_contents.get('section_id')
        }

    def _process_item(self, content: dict, module_data: dict, course: dict) -> dict:
        """Process individual module item"""
        item_id = self.content_utilities.extract_item_id(content.get('filepath'), content.get('fileurl'))
        if item_id is None:
            self.event_logger.log_data(f'Unknown {self.modtype} {self.component_name} id', 
                f"Content type: {content['type']} fileurl: {content['fileurl']} content: {content}")
            return None
            
        item_url = content.get('fileurl', '')
        filename = self._extract_filename(item_url)
        
        item_html, item_type = self._get_item_content(item_url, content)
        
        # Use component name in field names
        item = {
            f'{self.component_name}_id': item_id,
            f'{self.component_name}_filename': filename,
            f'{self.component_name}_type': item_type,
            f'{self.component_name}_files': [],
            f'{self.component_name}_title': content.get('content', ''),
            f'{self.component_name}_filepath': content.get('filepath'),
            f'{self.component_name}_filesize': content.get('filesize'),
            f'{self.component_name}_fileurl': item_url,
            f'{self.component_name}_time_modified': content.get('timemodified'),
            f'{self.component_name}_sortorder': content.get('sortorder', 0),
            f'{self.component_name}_tags': content.get('tags', [])
        }
        
        # Add module data
        item.update(module_data)
        
        # Process HTML content if exists
        if item_html:
            output_path = os.path.join(self.data_store_path, course.get('idnumber'))
            processed_content = self.content_cleaner.process_html_content(
                item_html,
                output_path,
                self.modtype,
                str(module_data.get(f'{self.modtype}_cmid')),
                module_data.get(f'{self.modtype}_name', ''),
                str(item_id)
            )
            item.update(processed_content)
            
        return item


       
    def _extract_filename(self, url: str) -> str:
        """Extract filename from URL"""
        url_parts = urlparse(url)
        filename = url_parts.path.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        return filename
        
    def _get_item_content(self, item_url: str, content: dict) -> tuple:
        """Get item content and type"""
        if 'index.html' in item_url:
            return self.moodle_rest.get_moodle_web_file_content(item_url), 'html'
        return content.get('content'), 'file'
        

    def _process_item_usage(self, items: List[dict], module_data: dict) -> List[dict]:
        """Process item usage information"""
        items_with_file_check = []
        
        # Group items by ID
        unique_item_ids = set(item[f'{self.component_name}_id'] for item in items)
        
        for item_id in unique_item_ids:
            item_group = [i for i in items if i[f'{self.component_name}_id'] == item_id]
            
            html_item = next((i for i in item_group if i[f'{self.component_name}_type'] == 'html'), None)
            if html_item:
                html_item['is_used'] = module_data.get(f'{self.modtype}_visible', False)
                html_content = html_item.get('clean_html', '')
                items_with_file_check.append(html_item)
                
                file_items = [i for i in item_group if i[f'{self.component_name}_type'] == 'file']
                for file_item in file_items:
                    file_item['is_used'] = (
                        module_data.get(f'{self.modtype}_visible', False) and 
                        file_item[f'{self.component_name}_filename'] in html_content
                    )
                    items_with_file_check.append(file_item)
        
        return self.content_cleaner.clean_encoding_artifacts(
            self.content_cleaner.clean_escaped_slashes(items_with_file_check)
        )