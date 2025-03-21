from typing import Dict, List, Any
import pandas as pd
import json
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from lib.content_cleaners import content_cleaners
from lib.content_utilities import content_utilities
from lib.event_logger import EventLogger
import math
from datetime import datetime, timezone

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

    # Get and process the content of a module (course activity)
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


    # Call this method whenever a mod activity has subcomponents
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



    def process_forum_discussions(self, forum_discussions: Dict[str, List[Dict]], forum_data: Dict, course: Dict) -> List[Dict]:
        # List of fields to rename
        discussion_fields = [
            'id', 'name', 'groupid', 'timemodified', 'usermodified', 'timestart', 'timeend', 
            'discussion', 'parent', 'userid', 'created', 'modified', 'mailed', 'subject', 'message', 
            'messageformat', 'messagetrust', 'attachment', 'totalscore', 'mailnow', 'userfullname', 
            'usermodifiedfullname', 'userpictureurl', 'usermodifiedpictureurl', 'numreplies', 
            'numunread', 'pinned', 'locked', 'starred', 'canreply', 'canlock', 'canfavourite'
        ]
        
        # Prepend 'forum_discussion_' to field names
        prefixed_fields = {field: f"forum_discussion_{field}" for field in discussion_fields}
        
        # Extract discussions list (default to empty list if key is missing)
        discussions = forum_discussions.get('discussions', [])

        # If no discussions exist, return a single dict with all prefixed fields set to None
        if not discussions:
            return [{new_key: None for new_key in prefixed_fields.values()}]

        # Transform discussion dicts
        transformed_discussions = []
        for discussion in discussions:
            new_discussion = {
                prefixed_fields[key]: discussion[key] for key in discussion if key in prefixed_fields
            }
            # Merge forum_data into the discussion
            new_discussion.update(forum_data)
            transformed_discussions.append(new_discussion)

        return transformed_discussions

    
    def process_forum_discussion_posts(self, forum_posts: List[dict], discussion_data: dict, course: dict) -> List[dict]:
        # List of fields to rename
        post_fields = [
            'id', 'subject', 'replysubject', 'message', 'messageformat', 'author', 
            'discussionid', 'hasparent', 'parentid', 'timecreated', 'timemodified', 
            'unread', 'isdeleted', 'isprivatereply', 'haswordcount', 'wordcount', 
            'charcount', 'capabilities', 'urls', 'attachments', 'messageinlinefiles', 'tags', 'html'
        ]
        
        # Prepend 'forum_post_' to field names
        prefixed_fields = {field: f"forum_post_{field}" for field in post_fields}
        
        # Extract posts list (default to empty list if key is missing)
        posts = forum_posts.get('posts', [])

        # If no posts exist, return a single dict with all prefixed fields set to None
        if not posts:
            return [{new_key: None for new_key in prefixed_fields.values()}]

        # Transform post dicts
        transformed_posts = []
        for post in posts:
            new_post = {
                prefixed_fields[key]: post[key] for key in post if key in prefixed_fields
            }
            # Merge forum_discussion_data into the post
            new_post.update(discussion_data)
            transformed_posts.append(new_post)

        return transformed_posts


    def _create_base_module_data(self, module_contents: dict, course: dict) -> dict:
        """Create base module data dictionary"""
        module_data = {
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
        module_data = self.content_cleaner.check_module_data(module_data) # Remove any newlines from any field if it has html content
        return module_data

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
        if self.component_name == 'chapter':
            
            item[f'{self.component_name}_url'] = f"{module_data.get('book_url')}&chapter={item.get('chapter_id')}"

            # Adding Moodle url to text editor where - the file manager can be opened and references to unused files deleted.
            #
            # Mdl txt editor URL example https://example.com/mod/book/edit.php?cmid=58392&id=100115
            item[f'{self.component_name}_editor_url'] = f"https://learn.rvc.ac.uk/mod/book/edit.php?cmid={int(module_data.get(f'{self.modtype}_cmid'))}&id={item.get('chapter_id')}"


            # Human readable filesize - consider refactoring to use (currently unused) fn below called _convert_size
            # 
            size_bytes = int(item['chapter_filesize'])

            if size_bytes == 0: 
                human_size = "0B" 
            else: 
                size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB") 
                i = int(math.floor(math.log(size_bytes, 1024)))
                power = math.pow(1024, i) 
                size = round(size_bytes / power, 2) 
                human_size = "{} {}".format(size, size_name[i])

            item[f'{self.component_name}_filesize_readable'] = f"{human_size}"

            # Human readable time modified
            #
            ts = int(item['chapter_time_modified'])
            x = datetime.fromtimestamp(ts, tz=timezone.utc)
            item[f'{self.component_name}_time_modified_readable'] = x.strftime('%Y-%m-%d %H:%M')

        
        
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
    

    def _convert_size(size_bytes, ignore2ndposarg): 
        """
        original fn taken from https://python-forum.io/thread-6709.html
        TODO refactor to use a convert size fn here rather than including code within the  _process_item fn
        """
        if size_bytes == 0: 
            return "0B" 
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB") 
        i = int(math.floor(math.log(size_bytes, 1024)))
        power = math.pow(1024, i) 
        size = round(size_bytes / power, 2) 
        return "{} {}".format(size, size_name[i])