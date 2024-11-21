from typing import Tuple, Dict, List, Any
import pandas as pd
import base64
import json
import os
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
from lib.event_logger import EventLogger


class content_cleaners:
    def __init__(self) -> None:
        self.event_logger = EventLogger()


    def clean_text(self, text):
        import re
        from html import unescape
        
        # Handle unicode escapes first
        try:
            text = text.encode('utf-8').decode('unicode-escape')
        except:
            pass
        
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
            return block_data.replace('Â ', '')
        return block_data

    def extract_and_save_embedded_images(self, html_content: str, output_path: str, 
                                    content_source: str, object_cmid: str, object_name: str, 
                                    chapter_id: str) -> str:
        """
        Extract base64 embedded images, save them, and replace with localhost links.
        Logs any encountered non-image data: types.
        
        Args:
            html_content: HTML content containing embedded images
            output_path: Base path for saving images (course_data/course_idnumber/)
            content_source: Source type (e.g., 'book', 'page', etc.)
            object_cmid: CMID of the content object
            object_name: Name of the content object
            chapter_id: Chapter ID
            
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
                                f"(cmid: {object_cmid}, chapter: {chapter_id}): {data_type}")
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
                filename = f"{object_cmid}_{clean_object_name}_{chapter_id}_{image_count}.{image_format}"
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
                    error = f"Error processing image in {content_source} {object_name} cmid {object_cmid} chapter {chapter_id}: {str(e)}"
                    self.event_logger.log_data(f'Error processing {content_source} embedded image', error)
                    continue
        
        return str(soup)