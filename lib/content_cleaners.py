from typing import Dict, List, Any
import pandas as pd
import httpx
import json
import csv
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs


class content_cleaners:
    def __init__(self) -> None:
        pass


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
