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

class mod_book:
    def __init__(self, moodle_rest) -> None:
        self.content_cleaner = content_cleaners()
        self.content_utilities = content_utilities()
        self.moodle_rest = moodle_rest


    def process_book_chapters(self, contents: List[dict], book: dict) -> List[dict]:
        """
        Process book chapters to extract and clean HTML content and associate files.
        
        Args:
            contents: List of content items from Moodle book module
            
        Returns:
            Tuple of (processed chapters list, files usage dictionary)
        """
        chapters = []

        # Skip first item as it contains TOC
        for content in contents[1:]:
            if content['type'] != 'file' or 'index.html' not in content['fileurl']:
                continue
                
            # Extract chapter number from filepath
            filepath = content['filepath'].rstrip('/')  # Remove trailing slash
            parts = filepath.split('/')  # Split the path into parts

            chapter_id = None  # Default value in case no valid integer is found

            # Take the last integer value in the filepath
            for part in reversed(parts):
                try:
                    chapter_id = int(part)  # Attempt to convert the part to an integer
                    break  # Stop once the last valid integer is found
                except ValueError:
                    continue  # Ignore non-integer parts

            if chapter_id is None:
                continue
            chapter_url = content.get('fileurl')
            chapter_html = self.moodle_rest.get_book_chapter(chapter_url)
            chapter = {
                'chapter_id': chapter_id,
                'chapter_content': chapter_html,
                'files': [],
                'title': content.get('content', ''),  # Chapter title from content
                'filepath': content['filepath'],
                'fileurl': chapter_url,
                'time_modified': content.get('timemodified'),
                'sortorder': content.get('sortorder', 0),
                'tags': content.get('tags', [])
            }
            # Add each field of book to chapter
            for key, value in book.items():
                chapter[key] = value
            
            # Process HTML content if present
            if chapter.get('chapter_content'):
                soup = BeautifulSoup(chapter['chapter_content'], 'html.parser')
                
                # Process media and links
                for tag in soup.find_all(['img', 'video', 'audio', 'source', 'a']):
                    # Handle src/href attributes
                    attr = 'href' if tag.name == 'a' else 'src'
                    if url := tag.get(attr):
                        # Clean URL
                        cleaned_url = self.content_cleaner.clean_url(url)
                        tag[attr] = cleaned_url
                        
               
                # Clean the HTML content
                cleaned_html = str(soup)
                cleaned_text = self.content_cleaner.clean_text(soup.get_text(separator=' '))
                
                chapter['clean_html'] = cleaned_html
                chapter['clean_text'] = cleaned_text
            
            
            chapters.append(chapter)
        
        # Sort chapters by number 
        chapters.sort(key=lambda x: int(x['chapter_id']))
        
        # Final cleanup of any encoding artifacts
        chapters = self.content_cleaner.clean_encoding_artifacts(chapters)
        chapters = self.content_cleaner.clean_escaped_slashes(chapters)
        
        return chapters

    # This gets all books but not all book content for a course via course_resources
    def get_book_content(self, course_modules, course, course_resources):
        """
        Extract contents from book modules and combine with course information.
        
        Args:
            course_modules: DataFrame containing module information
            course: Dictionary containing course metadata
            course_resources: DataFrame containing resource information
            
        Returns:
            DataFrame with book metadata and extracted contents
        """
        book_modules = course_modules[course_modules['modname'] == 'book']
        book_contents = self.moodle_rest.get_mod_books_in_course(course.get('id'))
        # print(book_contents)
        results = []

        for _, book_contents in book_modules.iterrows():
            book_data = {
                'course_id': course.get('id'),
                'course_name': course.get('fullname'),
                'book_id': book_contents.get('instance'),
                'book_cmid': book_contents.get('id'),
                'book_name': book_contents.get('name'),
                'book_description': book_contents.get('description'),
                'book_contextid': book_contents.get('contextid'),
                'book_visible': book_contents.get('visible'),
                'book_url': book_contents.get('url'),
                'book_section_id': book_contents.get('section_id')
            }

            contents = book_contents.get('contents', [])

            # Get TOC from first content item
            toc = []
            if contents and contents[0].get('type') == 'content':
                try:
                    toc = json.loads(contents[0]['content'])
                except:
                    toc = []

            # Process chapters and get file usage info
            chapters = self.process_book_chapters(contents, book_data)
            
            book_data.update({
                'toc': toc,
                'chapters': chapters
            })
            
            results.append(book_data)

        return pd.DataFrame(results)