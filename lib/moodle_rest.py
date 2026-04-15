import pandas as pd
import httpx
import json
import os
from dotenv import load_dotenv
from lib.event_logger import EventLogger
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import requests
from bs4 import BeautifulSoup

class MoodleRESTError(Exception):
    """Custom exception for Moodle REST API errors"""
    pass

class DatabaseConnectionError(MoodleRESTError):
    """Raised when database connection issues occur"""
    pass

class moodle_rest:
    def __init__(self, use_uat=False):
        load_dotenv(override=True)
        self.use_uat = use_uat
        if self.use_uat:
            self.moodle_api_token = os.getenv('UAT_MOODLE_TOKEN')
            self.moodle_url = os.getenv('UAT_MOODLE_URL')
            self.moodle_user = os.getenv('UAT_MOODLE_USER')
            self.moodle_password = os.getenv('UAT_MOODLE_PASSWORD')
            self.configured_rest_service_name = os.getenv('UAT_MOODLE_SERVICE_NAME', 'configured token')
            self.rest_token_source = os.getenv('UAT_MOODLE_REST_TOKEN_SOURCE', 'configured').strip().lower()
        else:
            self.moodle_api_token = os.getenv('MOODLE_TOKEN')
            self.moodle_url = os.getenv('MOODLE_URL')
            self.moodle_user = os.getenv('MOODLE_USER')
            self.moodle_password = os.getenv('MOODLE_PASSWORD')
            self.configured_rest_service_name = os.getenv('MOODLE_SERVICE_NAME', 'configured token')
            self.rest_token_source = os.getenv('MOODLE_REST_TOKEN_SOURCE', 'configured').strip().lower()


        self.rest_endpoint = '/webservice/rest/server.php'
        self.mobile_service_name = 'moodle_mobile_app'
        self.event_logger = EventLogger()
        self.headers = {"Accept": "application/json"}
        self.timeout = 60
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.moodle_web_session = None
        self.active_rest_token = self.moodle_api_token
        self.active_rest_service_name = self.configured_rest_service_name if self.moodle_api_token else None

        # Initialize connection
        self.initialize_connection()

  
    def initialize_connection(self):
        """Initialize connection and required data with retry logic"""
        try:
            self.moodle_mobile_token = self.get_moodle_mobile_token()
            self.select_rest_token()

            self.event_logger.log_data(
                "rest_service_selected",
                f"Using Moodle REST service '{self.active_rest_service_name}'"
            )
            print(f"Using Moodle REST service: {self.active_rest_service_name}")

            self.moodle_courses = None
            self.current_course = None
            self.current_course_blocks = None
            self.current_course_modules = None
            self.current_course_sections = None
            self.current_course_content = None
            self.current_course_resources = None
        except Exception as e:
            self.event_logger.log_data("initialization_error", f"Failed to initialize Moodle connection: {str(e)}")
            raise

    def select_rest_token(self):
        sources = {
            'configured': (self.moodle_api_token, self.configured_rest_service_name),
            'mobile': (self.moodle_mobile_token, self.mobile_service_name),
        }

        if self.rest_token_source == 'auto':
            preferred_sources = ['configured', 'mobile']
        elif self.rest_token_source in sources:
            preferred_sources = [self.rest_token_source, 'mobile' if self.rest_token_source == 'configured' else 'configured']
        else:
            raise MoodleRESTError(
                f"Invalid Moodle REST token source '{self.rest_token_source}'. Use configured, mobile, or auto."
            )

        for source_name in preferred_sources:
            token, service_name = sources[source_name]
            if token:
                self.active_rest_token = token
                self.active_rest_service_name = service_name
                return

        raise MoodleRESTError("No Moodle REST token available. Set MOODLE_TOKEN or ensure username/password can obtain a service token.")

    # Returns a tuple of web token and api token but only one will be set, with the priority being mobile app token
    def get_moodle_mobile_token(self):
        response = httpx.get(
            f"{self.moodle_url}/login/token.php",
            params={
                'username': self.moodle_user,
                'password': self.moodle_password,
                'service': self.mobile_service_name,
            },
            timeout=60
        )
        if response.status_code == 200:
            if response.json().get('error'):
                self.event_logger.log_data(
                    "mobile_token_unavailable",
                    f"Failed to get {self.mobile_service_name} token: {response.json().get('error')}"
                )
                if self.moodle_api_token is None:
                    print(f"Failed to get token: {response.json().get('error')} - most probably the cause is {self.mobile_service_name} is not enabled.")
                    print("Now attempting to get a web login, which requires MOODLE_USER and MOODLE_PASSWORD to be set in .env")
                    self.get_moodle_session()
                return None
            return response.json().get('token')
        else:
            raise Exception(f"Failed to get token: {response.text}")
    
    # Note we only request moodle_session if mobile token failed, probably due to mobile app not enabled
    def get_moodle_session(self):
        session = requests.Session()
        response = session.get(
            f"{self.moodle_url}/login/index.php",
            timeout=60)
        if response.status_code != 200:
            raise Exception(f"Failed to get user session: {response.text}")
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            logintoken = soup.find('input', {'name': 'logintoken'})['value']
            self.moodle_web_session = session
        except Exception as e:
            raise Exception(f"Failed to get logintoken, check .env username and password : {str(e)}")
        
        login_data = {
            'username': self.moodle_user,
            'password': self.moodle_password,
            'logintoken': logintoken
        }
        login_response = session.post(f"{self.moodle_url}/login/index.php", data=login_data)
        if "Invalid login" in login_response.text:
            raise Exception(f"Failed to login: {login_response.text[:200]}")
        else:
            print("Successfully logged in so now using web requests rather than mobile requests")
        return

        
    def get_courses(self):
        if self.moodle_courses is None:
            response = self.get_moodle_rest_request('core_course_get_courses_by_field')
            self.moodle_courses = pd.DataFrame(response.get('courses', []))
        return self.moodle_courses

    def cache_courses(self, courses_df):
        if courses_df is None or courses_df.empty:
            return courses_df

        if self.moodle_courses is None or self.moodle_courses.empty:
            self.moodle_courses = courses_df.copy()
        else:
            self.moodle_courses = pd.concat([self.moodle_courses, courses_df], ignore_index=True)
            self.moodle_courses = self.moodle_courses.drop_duplicates(subset=['id'], keep='last')
        return courses_df

    def get_courses_by_field(self, field=None, value=None):
        parameters = {}
        if field not in [None, '']:
            parameters['field'] = field
        if value not in [None, '']:
            parameters['value'] = value

        response = self.get_moodle_rest_request('core_course_get_courses_by_field', **parameters)
        courses = pd.DataFrame(response.get('courses', []))
        return self.cache_courses(courses)

    def search_courses(self, value, page=0, perpage=0):
        if value in [None, '']:
            return pd.DataFrame()

        response = self.get_moodle_rest_request(
            'core_course_search_courses',
            criterianame='search',
            criteriavalue=value,
            page=page,
            perpage=perpage,
        )
        courses = pd.DataFrame(response.get('courses', []))
        return self.cache_courses(courses)

    # Get the html content of a Moodle file (index.html) object
    def get_moodle_web_file_content(self, moodle_file_url):
        try:

            if self.active_rest_token is None:
                moodle_file_url = moodle_file_url.replace('/webservice', '')  # Remove /webservice from URL
                if self.moodle_web_session is None:
                    raise Exception("No session available for web requests")
                response = self.moodle_web_session.get(
                    moodle_file_url,
                    timeout=60  # 1 minutes in seconds
                )
                if "login" in response.url:
                    raise Exception("Login required to access file - alter this code to deal with that failure!")

            else:
                token_param = f'token={self.active_rest_token}'

                separator = '&' if '?' in moodle_file_url else '?'
                response = httpx.get(
                    f'{moodle_file_url}{separator}{token_param}',
                    timeout=300  # 5 minutes in seconds
                )
            
            response.raise_for_status()
            # we should have a reponse by now
            try:
                # Try to parse as JSON
                json_content = response.json()
                # If it's JSON and has error, log the error
                if 'error' in json_content:
                    self.event_logger.log_data("get moodle web file content error", f"Error in response: {json_content['error']}")
                # If it's JSON but no error (shouldn't happen), fall through to return text
            except ValueError:
                # Not JSON, so must be HTML - this is the expected case
                pass
            
            # Return the raw text content in all success cases
            return response.text
            
        except Exception as e:
            self.event_logger.log_data("Unknown error getting html moodle content", f"Error getting moodle file content for {moodle_file_url}: {str(e)}")
            return None


    def get_forum_discussions(self, forum_id):
        response = self.get_moodle_rest_request('mod_forum_get_forum_discussions', forumid=forum_id)
        return response

    def get_forum_discussion_posts(self, discussion_id):
        response = self.get_moodle_rest_request('mod_forum_get_discussion_posts', discussionid=discussion_id)
        return response

    def get_mod_books_in_course(self, course_id):
        response = self.get_moodle_rest_request('mod_book_get_books_by_courses', courseids=[course_id])
        return response

    def get_course(self, course_id):
        try:
            if self.moodle_courses is None or self.moodle_courses.empty:
                self.get_courses()

            if 'id' not in self.moodle_courses.columns or course_id not in self.moodle_courses['id'].values:
                self.get_courses_by_field('id', course_id)

            course = self.moodle_courses.loc[self.moodle_courses['id'] == course_id]
            return course.iloc[0]
        except:
            return None
        
    # general "search course fields for value" and if field valid name and match found return course
    def get_course_by(self, field='id', value=None):
        try:
            if self.moodle_courses is None or self.moodle_courses.empty:
                self.get_courses()
            course = self.moodle_courses.loc[self.moodle_courses[field] == value]
            return course.iloc[0]
        except:
            return None

    def set_course(self, course_id: int) -> Optional[Dict[str, Any]]:
        """Set current course with enhanced error handling"""
        try:
            self.current_course = course_id
            
            # Get course blocks with retry
            blocks_response = self.get_moodle_rest_request('core_block_get_course_blocks', courseid=course_id)
            self.current_course_blocks = pd.DataFrame(blocks_response['blocks'])
            
            # Get course content with retry
            content_response = self.get_moodle_rest_request('core_course_get_contents', courseid=course_id)
            self.current_course_content = pd.DataFrame(content_response)
            
            # Get resources with retry
            resources_response = self.get_moodle_rest_request('mod_resource_get_resources_by_courses', courseids=[course_id])
            self.current_course_resources = pd.DataFrame(resources_response['resources'])
            
            # Process course modules
            course_modules = pd.DataFrame()
            for _, course_section in self.current_course_content.iterrows():
                modules_to_add = pd.DataFrame(course_section['modules'])
                modules_to_add['section_id'] = course_section['id']
                course_modules = pd.concat([course_modules, modules_to_add], ignore_index=True)
            
            self.current_course_sections = self.current_course_content.drop(columns=['modules'])
            self.current_course_modules = course_modules
            self.current_course_blocks = self.get_block_content(course_id)
            
            return self.get_course(course_id)
            
        except Exception as e:
            self.event_logger.log_data("set_course_error", f"Error setting course {course_id}: {str(e)}")
            raise

    def get_matching_courses(self, field='id', value=None):
        if value in [None, '']:
            return None

        if "*" in value:
            pattern = value.replace("*", ".*")
            if self.moodle_courses is None:
                self.get_courses()
            try:
                courses = self.moodle_courses[self.moodle_courses[field].str.match(pattern, na=False)]
                return courses
            except:
                return None
        else:
            try:
                courses = self.get_courses_by_field(field, value)
                return courses
            except:
                return None

    def get_matching_courses_from_list(self, field='id', list_of_values=None):
        courses = pd.DataFrame()  # Initialize an empty DataFrame to store matching courses
        for value in list_of_values:
            try:
                matched_courses = self.get_matching_courses(field, value)
                if matched_courses is None or matched_courses.empty:
                    continue
                courses = pd.concat([courses, matched_courses], ignore_index=True)
            except Exception as e:
                self.event_logger.log_data("get_matching_courses_from_list_error", f"Error matching course for value {value}: {str(e)}")
        if courses.empty:
            return None
        return courses.drop_duplicates(subset=['id'], keep='last')

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

    def check_database_error(self, response_data: Dict[str, Any]) -> None:
        """Check for database-related errors in the response"""
        if isinstance(response_data, dict) and "exception" in response_data:
            error_msg = response_data.get('message', '')

            # <<< add this block to swallow that specific ODBC argument error >>>
            if 'Argument #1 ($odbc)' in error_msg:
                self.event_logger.log_data("db_odbc_argument1_error", error_msg)
                return True

            if 'odbc_exec' in error_msg:
                raise DatabaseConnectionError(f"Database connection error: {error_msg}")
            elif "Can't find data record in database" in error_msg:
                return True

            raise MoodleRESTError(f"Moodle API Error: {error_msg}")
        return False


    # Call Moodle API - note does not throw exception on error
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(DatabaseConnectionError),
    )
    def get_moodle_rest_request(self, moodle_function: str, **kwargs) -> Dict[str, Any]:
        """
        Enhanced Moodle REST API request with retry logic and better error handling
        """
        if not self.active_rest_token:
            raise MoodleRESTError("No active Moodle REST token is available for API requests.")

        parameters = dict(self.flatten_api_parameters(kwargs))
        parameters.update({
            "wstoken": self.active_rest_token,
            "moodlewsrestformat": "json",
            "wsfunction": moodle_function
        })

        try:
            response = httpx.get(
                self.moodle_url + self.rest_endpoint,
                params=parameters,
                headers=self.headers,
                timeout=120
            )
            
            # Log non-200 responses
            if response.status_code != 200:
                self.event_logger.log_data(
                    "api_error",
                    f"Non-200 response: {response.status_code} - {response.text}"
                )
                response.raise_for_status()

            response_data = response.json()
            
            # Check for database errors
            had_error = self.check_database_error(response_data)
            if had_error:
                return {} # Nothing to return
            
            return response_data

        except DatabaseConnectionError:
            # Log database errors and retry
            self.event_logger.log_data("database_error", "Database connection issue detected")
            # Sleep before retry
            time.sleep(self.retry_delay)
            raise  # Let retry decorator handle it

        except MoodleRESTError:
            raise
            
        except httpx.RequestError as e:
            self.event_logger.log_data("request_error", f"Request failed: {str(e)}")
            raise MoodleRESTError(f"Request Error: {str(e)}")
            
        except json.JSONDecodeError as e:
            self.event_logger.log_data("json_error", f"JSON decode error: {str(e)}")
            raise MoodleRESTError(f"JSON Decode Error: {str(e)}")
            
        except Exception as e:
            self.event_logger.log_data(
                "unexpected_error",
                f"Unexpected error with service {self.active_rest_service_name}, parameters: {parameters}, error: {str(e)}"
            )
            raise MoodleRESTError(f"Unexpected Error: {str(e)}")
