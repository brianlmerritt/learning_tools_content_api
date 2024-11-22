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
from lib.event_logger import EventLogger

class mod_book:
    def __init__(self, moodle_rest) -> None:
        self.content_cleaner = content_cleaners()
        self.content_utilities = content_utilities()
        self.moodle_rest = moodle_rest
        self.event_logger = EventLogger()


    def process_book_chapters(self, contents: List[dict], book: dict, course: dict) -> List[dict]:
        chapters = []

        for content in contents[1:]:
            if content['type'] != 'file':
                self.event_logger.log_data('Unknown book chapter type', f"Content type: {content['type']} fileurl: {content['fileurl']} content: {content}")
                continue
            
            # Extract chapter ID from filepath
            filepath = content['filepath'].rstrip('/')
            parts = filepath.split('/')

            chapter_id = None
            for part in reversed(parts):
                try:
                    chapter_id = int(part)
                    break
                except ValueError:
                    continue

            if chapter_id is None:
                self.event_logger.log_data('Unknown book chapter id', f"Content type: {content['type']} fileurl: {content['fileurl']} content: {content}")
                continue

            # Extract filename from chapter_url
            chapter_url = content.get('fileurl', '')
            url_parts = urlparse(chapter_url)
            filename = url_parts.path.split('/')[-1]
            if '?' in filename:
                filename = filename.split('?')[0]

            if 'index.html' in chapter_url:
                chapter_html = self.moodle_rest.get_book_chapter(chapter_url)
                chapter_type = 'html'
            else:
                chapter_html = content.get('content')
                chapter_type = 'file'
                
            chapter = {
                'chapter_id': chapter_id,
                'chapter_filename': filename,
                'chapter_content': chapter_html,
                'chapter_type': chapter_type,
                'files': [],
                'title': content.get('content', ''),
                'filepath': content.get('filepath'),
                'filesize': content.get('filesize'),
                'fileurl': chapter_url,
                'time_modified': content.get('timemodified'),
                'sortorder': content.get('sortorder', 0),
                'tags': content.get('tags', [])
            }

            for key, value in book.items():
                chapter[key] = value
            
            if chapter.get('chapter_content'):
                if isinstance(chapter['chapter_content'], str):
                    chapter['chapter_content'] = chapter['chapter_content'].encode('ascii', 'ignore').decode('ascii')
                
                output_dir = f"course_data/{course.get('idnumber')}/"
                chapter['chapter_content'] = self.content_cleaner.extract_and_save_embedded_images(
                    chapter['chapter_content'],
                    output_dir,
                    'book',
                    str(book.get('book_cmid')),
                    book.get('book_name', ''),
                    str(chapter_id)
                )
                
                soup = BeautifulSoup(chapter['chapter_content'], 'html.parser')
                
                # Process URLs
                for tag in soup.find_all(['img', 'video', 'audio', 'source', 'a']):
                    attr = 'href' if tag.name == 'a' else 'src'
                    if url := tag.get(attr):
                        cleaned_url = self.content_cleaner.clean_url(url)
                        tag[attr] = cleaned_url
                
                # Create cleanest version by removing styling and scripts
                cleanest_soup = BeautifulSoup(str(soup), 'html.parser')
                # Remove all style and script tags
                for tag in cleanest_soup.find_all(['style', 'script']):
                    tag.decompose()
                # Remove all style attributes and classes
                for tag in cleanest_soup.find_all():
                    if tag.attrs:
                        tag.attrs = {k:v for k,v in tag.attrs.items() 
                                   if k in ['href', 'src', 'alt']}  # Keep only essential attributes
                
                cleaned_html = str(soup)
                cleanest_html = str(cleanest_soup)
                # Clean encoding
                cleaned_html = cleaned_html.encode('ascii', 'ignore').decode('ascii')
                cleanest_html = cleanest_html.encode('ascii', 'ignore').decode('ascii')
                cleaned_text = self.content_cleaner.clean_text(soup.get_text(separator=' '))
                if len(cleaned_html) > 32000:
                    cleaned_html = cleaned_html[:32000]
                    self.event_logger.log_data('Book chapter content truncated', f"Book cmid {book.get('book_cmid')} Chapter ID: {chapter_id} Content: {cleaned_html}")
                chapter['chapter_content'] = cleaned_html
                chapter['clean_html'] = cleaned_html
                chapter['cleanest_html'] = cleanest_html
                chapter['clean_text'] = cleaned_text
            chapters.append(chapter)
        
        chapters.sort(key=lambda x: int(x['chapter_id']))
        
        chapters_with_file_check = []
        unique_chapter_ids = set(chapter['chapter_id'] for chapter in chapters)
        
        for chapter_id in unique_chapter_ids:
            chapter_group = [c for c in chapters if c['chapter_id'] == chapter_id]
            
            html_chapter = next((c for c in chapter_group if c['chapter_type'] == 'html'), None)
            if html_chapter:
                html_chapter['is_used'] = book.get('book_visible', False)
                html_content = html_chapter.get('clean_html', '')
                chapters_with_file_check.append(html_chapter)
                
                file_chapters = [c for c in chapter_group if c['chapter_type'] == 'file']
                for file_chapter in file_chapters:
                    file_chapter['is_used'] = (book.get('book_visible', False) and 
                                            file_chapter['chapter_filename'] in html_content)
                    chapters_with_file_check.append(file_chapter)
        
        chapters = chapters_with_file_check


        #for chapter in chapters:
        #    chapter['is_used'] = False
        #    if chapter['chapter_type'] == 'html':
        #        chapter['is_used'] = book.get('book_visible', False)
        #        
        #        if chapter.get('clean_html'):
        #            soup = BeautifulSoup(chapter['clean_html'], 'html.parser')
        #            hrefs = [a.get('href', '') for a in soup.find_all('a')]
        #            
        #            for other_chapter in chapters:
        #                if (other_chapter['chapter_type'] == 'file' and 
        #                    other_chapter['chapter_id'] == chapter['chapter_id'] and
        #                    other_chapter['chapter_filename'] and
        #                    any(other_chapter['chapter_filename'] in href for href in hrefs)):
        #                    other_chapter['is_used'] = book.get('book_visible', False)
        
        chapters = self.content_cleaner.clean_encoding_artifacts(chapters)
        chapters = self.content_cleaner.clean_escaped_slashes(chapters)
        
        return chapters


    def get_book_content(self, course_modules, course, course_resources):
            book_modules = course_modules[course_modules['modname'] == 'book']
            book_contents = self.moodle_rest.get_mod_books_in_course(course.get('id'))
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
                toc = []
                if contents and contents[0].get('type') == 'content':
                    try:
                        toc = json.loads(contents[0]['content'])
                    except:
                        toc = []

                chapters = self.process_book_chapters(contents, book_data, course)
                
                for chapter in chapters:
                    chapter_row = {
                        **book_data,
                        'toc': toc,
                        'chapter_id': chapter['chapter_id'],
                        'chapter_filename': chapter['chapter_filename'],  # Added this line
                        'chapter_filesize': chapter['filesize'],
                        'chapter_content': chapter['chapter_content'],
                        'chapter_type': chapter['chapter_type'],
                        'files': chapter['files'],
                        'title': chapter['title'],
                        'filepath': chapter['filepath'],
                        'fileurl': chapter['fileurl'],
                        'time_modified': chapter['time_modified'],
                        'sortorder': chapter['sortorder'],
                        'tags': chapter['tags'],
                        'clean_html': chapter.get('clean_html'),
                        'clean_text': chapter.get('clean_text'),
                        'is_used': chapter.get('is_used', False)  # Also adding is_used field
                    }
                    results.append(chapter_row)

            return pd.DataFrame(results)

