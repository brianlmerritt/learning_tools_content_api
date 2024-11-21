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