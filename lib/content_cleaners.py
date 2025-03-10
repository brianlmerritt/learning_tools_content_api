from typing import Tuple, Dict, List, Any
import pandas as pd
import base64
import json
import os
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
from lib.event_logger import EventLogger


URL_PATTERN = re.compile(
    r'^(https?://[^\s]+)$',  # Simple URL pattern matching http/https URLs
    re.IGNORECASE
)

class content_cleaners:
    def __init__(self) -> None:
        self.event_logger = EventLogger()

    def clean_text(self, text):
        import re
        from html import unescape
        # Handle unicode escapes first
        #try:
        #    text = text.encode('utf-8').decode('unicode-escape')
        #except:
        #    print(f"Error decoding unicode escape in {text}")
        # Remove escaped quotes at start/end if present
        text = text.strip('"\'')
        # Unescape HTML entities
        text = unescape(text)
        # Remove escaped HTML tags
        text = re.sub(r'<\\?/?\w+>', '', text)
        # Convert \r\n to spaces
        text = re.sub(r'\s*\\r\\n\s*', ' ', text)
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        # Final trim
        return text.strip()


    def clean_url(self, url):
        # Remove leading/trailing quotes of any type
        url = url.strip('"\'')
        # Remove any remaining escaped quotes
        url = url.replace('\\"', '')
        # Remove escaped slashes
        url = url.replace('\\/', '/')
        return url


    def clean_urls_in_dict(self, block_data):
        if isinstance(block_data, dict):
            if 'url' in block_data:
                block_data['url'] = self.clean_url(block_data['url'])
            return {k: self.clean_urls_in_dict(v) for k, v in block_data.items()}
        elif isinstance(block_data, list):
            return [self.clean_urls_in_dict(x) for x in block_data]
        return block_data


    def clean_escaped_slashes(self, block_data):
        """Clean any remaining escaped forward slashes in text content"""
        if isinstance(block_data, dict):
            return {k: self.clean_escaped_slashes(v) for k, v in block_data.items()}
        elif isinstance(block_data, list):
            return [self.clean_escaped_slashes(x) for x in block_data]
        elif isinstance(block_data, str):
            return block_data.replace('\\/', '/')
        return block_data


    def clean_encoding_artifacts(self, block_data):
        """Remove encoding artifacts like Â from text content"""
        if isinstance(block_data, dict):
            return {k: self.clean_encoding_artifacts(v) for k, v in block_data.items()}
        elif isinstance(block_data, list):
            return [self.clean_encoding_artifacts(x) for x in block_data]
        elif isinstance(block_data, str):
            return block_data.replace('Â ', '').replace('Â ', '').replace('Â ', '').replace('Â', '')
        return block_data

    def extract_and_save_embedded_images(self, html_content: str, output_path: str,
                                    content_source: str, object_cmid: str, object_name: str, 
                                    item_id: str) -> str:
        """
        Extract base64 embedded images, save them, and replace with localhost links.
        Logs any encountered non-image data: types.
        
        Args:
            html_content: HTML content containing embedded images
            output_path: Base path for saving images (course_data/course_idnumber/)
            content_source: Source type (e.g., 'book', 'page', etc.)
            object_cmid: CMID of the content object
            object_name: Name of the content object
            item_id: Subcomponent or Item ID
            
        Returns:
            Modified HTML with embedded images replaced by localhost links
        """
        if not html_content:
            return html_content
            
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Track number of images in this chapter
        image_count = 0
        
        # Process all img tags
        for img in soup.find_all('img'):
            src = img.get('src', '')
            
            # Check if it's a data: URL
            if src.startswith('data:'):
                # Extract the data type
                data_type = src.split(';')[0] if ';' in src else src
                
                # If it's not an image type, log it and continue
                if not data_type.startswith('data:image'):
                    log_message = (f"Unhandled data type in {content_source} {object_name} "
                                f"(cmid: {object_cmid}, chapter: {item_id}): {data_type}")
                    self.event_logger.log_data(f'Unhandled embedded content type', log_message)
                    continue
                
                # Process image data
                match = re.match(r'data:image/(\w+);base64,(.+)', src)
                if not match:
                    continue
                    
                image_format, base64_data = match.groups()
                image_count += 1
                
                # Clean object name for filename
                clean_object_name = re.sub(r'[^\w\-_]', '_', object_name)
                
                # Generate filename
                filename = f"{object_cmid}_{clean_object_name}_{item_id}_{image_count}.{image_format}"
                filepath = os.path.join(output_path, filename)
                
                try:
                    # Decode and save image
                    image_data = base64.b64decode(base64_data)
                    with open(filepath, 'wb') as f:
                        f.write(image_data)
                        
                    # Replace base64 data with localhost link
                    new_src = f"localhost://{output_path}/{filename}"
                    img['src'] = new_src
                    
                except Exception as e:
                    error = f"Error processing image in {content_source} {object_name} cmid {object_cmid} chapter {item_id}: {str(e)}"
                    self.event_logger.log_data(f'Error processing {content_source} embedded image', error)
                    continue
        
        return str(soup)
    
    def process_html_content(self, html_content: str, output_path: str, modtype: str, 
                            module_id: str = None, 
                            module_name: str = None, item_id: str = None) -> Dict[str, str]:
        """Process HTML content and return cleaned versions"""
        if not html_content:
            return {
                'content': '',
                'clean_html': '', 
                'cleanest_html': '', 
                'clean_text': ''
            }
        
        html_content = html_content.strip()

        # Detect if the entire content is just a URL
        if URL_PATTERN.match(html_content):
            # Early return for plain URL
            return {
                'content': html_content,
                'clean_html': html_content,
                'cleanest_html': html_content,
                'clean_text': html_content
            }


        html_content = html_content.encode('ascii', 'ignore').decode('ascii')
        
        # Extract and save embedded images
        html_content = self.extract_and_save_embedded_images(
            html_content, output_path, modtype, module_id, 
            module_name, item_id
        )
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Process URLs
        for tag in soup.find_all(['img', 'video', 'audio', 'source', 'a']):
            attr = 'href' if tag.name == 'a' else 'src'
            if url := tag.get(attr):
                cleaned_url = self.clean_url(url)
                tag[attr] = cleaned_url
        
        # Create cleanest version
        cleanest_soup = BeautifulSoup(str(soup), 'html.parser')
        for tag in cleanest_soup.find_all(['style', 'script']):
            tag.decompose()
        for tag in cleanest_soup.find_all():
            if tag.attrs:
                tag.attrs = {k:v for k,v in tag.attrs.items() 
                           if k in ['href', 'src', 'alt']}
        
        cleaned_html = str(soup)
        cleanest_html = str(cleanest_soup)
        cleaned_text = self.clean_text(soup.get_text(separator=' '))
        
        # Truncate if necessary
        if len(cleaned_html) > 32000:
            cleaned_html = cleaned_html[:32000]
            self.event_logger.log_data(
                f'{modtype} content truncated',
                f"{modtype} id {module_id} Item ID: {item_id} Content: {cleaned_html}"
            )
            
        return {
            'content': cleaned_html,
            'clean_html': cleaned_html,
            'cleanest_html': cleanest_html,
            'clean_text': cleaned_text
        }
    
    # Take module data (one row from the activity content) and if it has HTML content, remove carriage returns and line feeds
    def ai_check_module_data(self, module_data: dict) -> dict:
        """
        Iterate over each key/value pair in module_data.
        If a value is a string and contains embedded HTML (detected by the presence of '<' and '>'),
        remove all carriage returns and line feeds by replacing them with a single space and normalize whitespace.
        """
        cleaned_data = {}
        for key, value in module_data.items():
            if isinstance(value, str) and ('<' in value and '>' in value):
                # Replace any sequence of carriage returns or line feeds with a single space
                cleaned_value = re.sub(r'[\r\n]+', ' ', value)
                # Normalize all extra whitespace (tabs, multiple spaces, etc.) into a single space
                cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
                cleaned_data[key] = cleaned_value.strip()
            else:
                cleaned_data[key] = value
        return cleaned_data
    
    def check_module_data(self, module_data: dict) -> dict:
        """Remove any newlines and crap from any field """
        cleaned_data = {}
        for key, value in module_data.items():
                if isinstance (value, str):
                    cleaned_value = self.clean_text(value)
                    cleaned_value = re.sub(r'[\r\n]+', ' ', cleaned_value)
                    cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
                    cleaned_data[key] = cleaned_value.strip()
                else:
                    cleaned_data[key] = value
        return cleaned_data