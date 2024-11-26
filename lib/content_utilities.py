from typing import Dict, List, Tuple, Any
import pandas as pd
import httpx
import json
import csv
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs
from lib.content_cleaners import content_cleaners

class content_utilities:
    def __init__(self) -> None:
        self.content_cleaner = content_cleaners()


    def extract_resource_id(self, url: str) -> str:
        """Extract resource ID from pluginfile.php URL"""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        if 'pluginfile.php' in path_parts:
            try:
                return path_parts[path_parts.index('pluginfile.php') + 1]
            except IndexError:
                return None
        return None
    
    def extract_item_id(self, filepath: str, fileurl: str) -> int:
        """Extract item ID from filepath or fileurl"""
        
        # Let's try filepath first
        filepath = filepath.rstrip('/')
        parts = filepath.split('/')
        for part in reversed(parts):
            if part.isdigit():
                return int(part)
        
        # Check if fileurl contains 'webservice/pluginfile.php/' followed by a number
        if 'webservice/pluginfile.php/' in fileurl:
            number_part = fileurl.split('webservice/pluginfile.php/')[1].split('/')[0]
            if number_part.isdigit():
                return int(number_part)
                
        return None