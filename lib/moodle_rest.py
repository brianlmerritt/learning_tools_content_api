import pandas as pd
import httpx
import json
import os
from dotenv import load_dotenv

load_dotenv()

moodle_token = os.getenv('MOODLE_TOKEN')
moodle_url = os.getenv('MOODLE_URL')
moodle_user = os.getenv('MOODLE_USER')
moodle_password = os.getenv('MOODLE_PASSWORD')
idnumber_search = os.getenv('IDNUMBER_SEARCH')

class moodle_rest:
    def __init__(self):
        self.moodle_token = moodle_token
        self.moodle_url = moodle_url
        self.moodle_user = moodle_user
        self.moodle_password = moodle_password
        self.rest_endpoint = '/webservice/rest/server.php'
        response = httpx.get(
            f"{moodle_url}/login/token.php?username={moodle_user}&password={moodle_password}&service=moodle_mobile_app",
            timeout=300  # Set timeout to 5 minutes (300 seconds)
        )
        if response.status_code == 200:
            self.moodle_token = response.json().get('token')
        else:
            raise Exception(f"Failed to get token: {response.text}")
        self.moodle_courses = None
        self.current_course = None
        self.current_course_blocks = None
        self.current_course_modules = None
        self.current_course_sections = None
        self.current_course_content = None
        self.current_course_resources = None
        self.headers = {"Accept": "application/json"}
        self.timeout = 60 # let's not be too hasty
        self.get_courses()

  
    def get_courses(self):
        if self.moodle_courses is None:
            response = self.get_moodle_rest_request('core_course_get_courses')
            self.moodle_courses = pd.DataFrame(response)
        return self.moodle_courses

    
    def get_book_chapter(self, book_url):
        try:
            html = httpx.get(f'{book_url}?token={self.moodle_token}')
            return html.text
        except:
            print(f"Error getting book chapter {book_url}")
            return None


    def get_mod_books_in_course(self, course_id):
        response = self.get_moodle_rest_request('mod_book_get_books_by_courses', courseids=[course_id])
        return response

    def get_course(self, course_id):
        try:
            course = self.moodle_courses.loc[self.moodle_courses['id'] == course_id]
            return course.iloc[0]
        except:
            return None
        
    # general "search course fields for value" and if field valid name and match found return course
    def get_course_by(self, field='id', value=None):
        try:
            course = self.moodle_courses.loc[self.moodle_courses[field] == value]
            return course.iloc[0]
        except:
            return None

    def set_course(self, course_id):
        self.current_course = course_id
        self.current_course_blocks = pd.DataFrame(self.get_moodle_rest_request('core_block_get_course_blocks', courseid=course_id)['blocks'])
        self.current_course_content = pd.DataFrame(self.get_moodle_rest_request('core_course_get_contents', courseid=course_id))
        self.current_course_resources = pd.DataFrame(self.get_moodle_rest_request('mod_resource_get_resources_by_courses', courseids=[course_id])['resources'])
        # Create a new dataframe course_modules
        course_modules = pd.DataFrame()
        for _, course_section in self.current_course_content.iterrows():
            modules_to_add = pd.DataFrame(course_section['modules'])
            modules_to_add['section_id'] = course_section['id']
            course_modules = pd.concat([course_modules, modules_to_add], ignore_index=True)
        self.current_course_sections = self.current_course_content.drop(columns=['modules'])
        self.current_course_modules = course_modules
        self.current_course_blocks = self.get_block_content(course_id) # Append block_text and block_title to block content
        return self.get_course(course_id)

    def get_matching_courses(self, field='id', value=None):
        if "*" in value:
            pattern = value.replace("*", ".*")
            try:
                courses = self.moodle_courses[self.moodle_courses[field].str.match(pattern, na=False)]
                return courses
            except:
                return None
        else:
            try:
                courses = self.moodle_courses.loc[self.moodle_courses[field] == value]
                return courses
            except:
                return None

    # Note calling this with new course_id will update the current course
    def get_course_modules(self, course_id):
        if course_id == self.current_course:
            return self.current_course_modules
        else:
            self.set_course(course_id)
            return self.current_course_modules
        
    def get_course_sections(self, course_id):
        if course_id == self.current_course:
            return self.current_course_sections
        else:
            self.set_course(course_id)
            return self.current_course_sections
        
    # Note calling this with new course_id will update the current course
    def get_course_blocks(self, course_id):
        if course_id == self.current_course:
            return self.current_course_blocks
        else:
            self.set_course(course_id)
            return self.current_course_blocks
        
    def get_course_resources(self, course_id):
        if course_id == self.current_course:
            return self.current_course_resources
        else:
            self.set_course(course_id)
            return self.current_course_resources

    def extract_block_configs(self, block, key):
        #if block['name']  == 'html':
        configs = block.get('configs', None)
        if configs is not None and isinstance(configs, (list, tuple, set)):
            for item in configs:
                if item.get('name') == key:
                    return item['value']
        return None


    def get_block_content(self, course_id):
        blocks = pd.DataFrame(self.get_moodle_rest_request('core_block_get_course_blocks', courseid=course_id)['blocks'])
        blocks['block_title'] = None
        blocks['block_text'] = None
        # Apply the function only to rows where 'type' is 'html'
        blocks['block_title'] = blocks.apply(lambda row: self.extract_block_configs(row, 'title'), axis=1)
        blocks['block_text'] = blocks.apply(lambda row: self.extract_block_configs(row, 'text'), axis=1)
        return blocks
    
    def flatten_api_parameters(self, in_args, prefix=''):
        if isinstance(in_args, dict):
            flattened_params = {}
            for key, item in in_args.items():
                new_prefix = key if not prefix else f"{prefix}[{key}]"
                for sub_key, value in self.flatten_api_parameters(item, new_prefix).items():
                    flattened_params[sub_key] = value
            return flattened_params

        elif isinstance(in_args, list):
            flattened_params = {}
            for idx, item in enumerate(in_args):
                if isinstance(item, (dict, list)):
                    for key, value in self.flatten_api_parameters(item, f"{prefix}[{idx}]").items():
                        flattened_params[key] = value
                else:
                    flattened_params[f"{prefix}[{idx}]"] = item
            return flattened_params

        else:
            return {prefix: in_args} if prefix else {prefix: str(in_args)}

    # Call Moodle API - note does not throw exception on error
    def get_moodle_rest_request(self, moodle_function , **kwargs):
        parameters = dict(self.flatten_api_parameters(kwargs))
        parameters.update({
            "wstoken": self.moodle_token,
            "moodlewsrestformat": "json",
            "wsfunction": moodle_function
        })

        try:
            response = httpx.get(
                self.moodle_url + self.rest_endpoint,
                params=parameters,
                headers=self.headers,
                timeout=600  # Set timeout to 10 minutes (600 seconds)
            )
            response_data = response.json()
            # Check for Moodle API error responses
            if isinstance(response_data, dict) and "exception" in response_data:
                print(f"Moodle API Error: {response_data['message']}")
                print(f"Error Code: {response_data.get('errorcode')}")
                print(f"Debug Info: {response_data.get('debuginfo', 'No debug info available')}")
                raise Exception(f"Moodle API Error: {response_data['message']}")
            
            return response_data
        
        except httpx.RequestError as e:
            #print(f"Request Error: {str(e)}")
            raise Exception(f"Request Error: {str(e)}")
        except json.JSONDecodeError as e:
            #print(f"JSON Decode Error: {str(e)}")
            raise Exception(f"JSON Decode Error: {str(e)}")
        except Exception as e:
            print(f"Unexpected Error Parameters: {parameters}, response: {response_data}")
            raise Exception(f"Unexpected Error: {str(e)}")
        

       