import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import csv
import time
import getpass
import re
import os
from collections import deque
import ast

# --- Constants --- (MOODLE_BASE_URL, USERNAME, COURSE_ID would be from env or config)
LOGIN_PATH = "/login/index.php"
COURSE_VIEW_PATH = "/course/view.php" # Used to identify course home pages
OUTPUT_CSV_FILE = 'moodle_links.csv'
DELAY_SECONDS = 2
REQUEST_TIMEOUT = 30
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 UniversityLinkCrawler/1.0'

NON_RECURSIVE_LINK_TYPES = {
    "Course Link",
    "Calendar Link",
    "General Internal Link",
    "Gradebook Link",
    "User Profile Link",
    "Activity (glossary)"
}

# --- Global Variables ---
visited_urls = set()
csv_written_urls = set()
moodle_domain = "" # Will be set after getting base URL

def get_moodle_credentials():
    """Gets Moodle credentials from the user. Returns course_id as a string."""
    moodle_base_url = os.getenv("MOODLE_URL", "https://learn.rvc.ac.uk")
    username = os.getenv("MOODLE_USER", None)
    password = os.getenv("MOODLE_PASSWORD", None)
    if username is None or password is None:
        raise ValueError("Please set MOODLE_USER and MOODLE_PASSWORD environment variables.")
    idnumber_list = os.getenv("IDNUMBER_LIST", "[]")
    try:
        parsed_list = ast.literal_eval(idnumber_list)
        if isinstance(parsed_list, list) and parsed_list:
            course_id = str(parsed_list[0]) # Ensure course_id is a string
        else:
            raise ValueError("IDNUMBER_LIST is not a non-empty list or is malformed.")
    except Exception as e:
        raise ValueError(f"Failed to parse IDNUMBER_LIST: {e}")
    return moodle_base_url, username, password, course_id

def login_to_moodle(session, base_url, login_path, username, password):
    global moodle_domain
    parsed_base_url = urlparse(base_url)
    moodle_domain = parsed_base_url.netloc
    login_url = urljoin(base_url, login_path)
    print(f"Attempting to access login page: {login_url}")
    try:
        response = session.get(login_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        logintoken_tag = soup.find('input', {'name': 'logintoken'})
        logintoken = logintoken_tag['value'] if logintoken_tag and logintoken_tag.get('value') else None
        if logintoken: print("Found logintoken.")
        else: print("Warning: Could not find 'logintoken' field. Login might fail.")
        payload = {'username': username, 'password': password}
        if logintoken: payload['logintoken'] = logintoken
        print(f"Attempting to POST login credentials to {login_url}")
        response = session.post(login_url, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if response.url == login_url or "login/index.php" in response.url:
            soup_check = BeautifulSoup(response.text, 'html.parser')
            if soup_check.find(class_='loginerrors') or soup_check.find(id='loginerrormessage'):
                print("Login Failed! Check username/password. Error message found on page.")
                return False
            else:
                print("Warning: Still on login page after POST, but no explicit error found. Assuming login failed.")
                return False
        print("Login Successful! (heuristic check passed)")
        return True
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out while trying to connect to {login_url}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Error during login process: {e}")
        if 'response' in locals() and response: print("Response status:", response.status_code)
        return False
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return False

# MODIFICATION: Renamed original get_link_context
def get_html_based_link_context(link_tag):
    """
    Tries to determine where on the page the link was found by inspecting HTML parents.
    This is the fallback context detection method.
    """
    for parent in link_tag.parents:
        if parent.name == 'div' and parent.get('id') == 'region-main':
            if parent.find('ul', class_='topics') or parent.find('ul', class_='weeks'):
                activity_li = link_tag.find_parent('li', class_=lambda c: c and ('activity' in c.split() or 'resource' in c.split()))
                if activity_li:
                    return "Course Content Area (Activity/Resource)"
                else:
                    return "Course Content Area (General)"
            if parent.find(class_=re.compile(r'(?<!login)box|generalbox|description')):
                return "Activity/Resource Description"
            return "Course Content Area (Unknown Section)"
        if parent.name == 'div' and 'block' in parent.get('class', []):
            block_header = parent.find(['h2', 'h3', 'h4', 'h5', 'h6'], class_='card-title')
            block_title = block_header.get_text(strip=True) if block_header else "Unnamed Block"
            return f"Block ({block_title})"
        if parent.name == 'nav': return "Navigation Menu"
        if parent.name == 'header' or parent.get('id') == 'page-header': return "Page Header"
        if parent.name == 'footer' or parent.get('id') == 'page-footer': return "Page Footer"
        if parent.name == 'body': break
    return "Unknown Location"

def classify_link(url):
    path = urlparse(url).path
    if '/mod/' in path:
        parts = path.split('/')
        try: mod_index = parts.index('mod'); activity_type = parts[mod_index + 1]; return f"Activity ({activity_type})"
        except (IndexError, ValueError): return "Activity (Unknown Type)"
    elif COURSE_VIEW_PATH in path: return "Course Link" # More specific check
    elif '/user/profile.php' in path: return "User Profile Link"
    elif '/grade/report' in path: return "Gradebook Link"
    elif '/calendar/view.php' in path: return "Calendar Link"
    else: return "General Internal Link"

def is_page_on_target_course_from_soup(soup, target_course_id):
    breadcrumb_ul = soup.find('ul', class_='breadcrumb')
    if not breadcrumb_ul: return False
    breadcrumb_links = breadcrumb_ul.find_all('a', href=True)
    for bc_link_tag in breadcrumb_links:
        href = bc_link_tag['href']
        parsed_href = urlparse(href)
        if COURSE_VIEW_PATH in parsed_href.path:
            query_params = parse_qs(parsed_href.query)
            if 'id' in query_params and query_params['id'][0] == target_course_id:
                return True
    return False

def crawl_page(session, base_url, start_url, writer, target_course_id):
    global csv_written_urls, visited_urls
    queue = deque([(start_url, "Initial Entry")])

    while queue:
        current_url, parent_url = queue.popleft()
        current_url_normalized = current_url.strip().split('#')[0]
        if not current_url_normalized: continue
        if current_url_normalized in visited_urls: continue

        print(f"Crawling: {current_url_normalized} (from: {parent_url})")
        visited_urls.add(current_url_normalized)
        parsed_current_url = urlparse(current_url_normalized)

        if parsed_current_url.scheme not in ['http', 'https'] or parsed_current_url.netloc != moodle_domain:
            print(f"Skipping non-HTTP or external URL: {current_url_normalized}")
            continue

        try:
            time.sleep(DELAY_SECONDS)
            response = session.get(current_url_normalized, timeout=REQUEST_TIMEOUT, headers={'User-Agent': USER_AGENT})
            response.raise_for_status()
            content_type = response.headers.get('content-type', '').lower()

            if 'text/html' not in content_type:
                print(f"Skipping non-HTML content at {current_url_normalized} (Type: {content_type})")
                if current_url_normalized not in csv_written_urls:
                    writer.writerow([parent_url, current_url_normalized, "N/A (Non-HTML)", "Non-HTML Resource", "N/A"])
                    csv_written_urls.add(current_url_normalized)
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # --- Determine characteristics of the current page ---
            current_page_type_str = classify_link(current_url_normalized)
            is_current_page_target_course_home_flag = False
            if current_page_type_str == "Course Link": # Checks if it's a /course/view.php page
                page_id_param = parse_qs(parsed_current_url.query).get('id', [''])[0]
                if page_id_param == target_course_id:
                    is_current_page_target_course_home_flag = True
            
            is_current_page_in_target_course_context_flag = is_current_page_target_course_home_flag or \
                                                            is_page_on_target_course_from_soup(soup, target_course_id)
            
            # Context message for the page being crawled
            if is_current_page_in_target_course_context_flag:
                 print(f"  Page Context: Current page '{current_url_normalized}' (Type: {current_page_type_str}) is part of target course '{target_course_id}'.")
            else:
                 print(f"  Page Context: Current page '{current_url_normalized}' (Type: {current_page_type_str}) not confirmed part of target course '{target_course_id}'. Activity links from here may not be recursed.")
            # ---

            links = soup.find_all('a', href=True)
            for link_tag in links:
                href = link_tag['href'].strip()
                if not href or href.startswith('#') or href.startswith('javascript:'): continue

                absolute_url = urljoin(current_url_normalized, href).split('#')[0]
                link_text = link_tag.get_text(strip=True)
                
                # Skip self-links immediately after full URL construction
                if absolute_url == current_url_normalized:
                    continue

                parsed_absolute = urlparse(absolute_url)
                if parsed_absolute.scheme not in ['http', 'https'] or parsed_absolute.netloc != moodle_domain:
                    if absolute_url not in csv_written_urls:
                        writer.writerow([current_url_normalized, absolute_url, link_text, "External Link", "N/A"])
                        csv_written_urls.add(absolute_url)
                    continue

                if 'logout' in absolute_url.lower():
                    if absolute_url not in csv_written_urls:
                        writer.writerow([current_url_normalized, absolute_url, link_text, "Logout Link", "N/A"])
                        csv_written_urls.add(absolute_url)
                    visited_urls.add(absolute_url)
                    continue

                link_type_str = classify_link(absolute_url)
                
                # --- MODIFIED CONTEXT DETERMINATION ---
                determined_context = None
                if link_type_str.startswith("Activity ("):
                    if is_current_page_target_course_home_flag:
                        determined_context = "Course Page (Activity List)"
                    elif current_page_type_str.startswith("Activity ("):
                        determined_context = "Activity Page (In-Activity Link)"
                
                if determined_context is None: # Fallback to HTML-based context
                    determined_context = get_html_based_link_context(link_tag)
                # --- END MODIFIED CONTEXT DETERMINATION ---

                if absolute_url not in csv_written_urls:
                    writer.writerow([current_url_normalized, absolute_url, link_text, link_type_str, determined_context])
                    print(f"  Found link (added to CSV): {absolute_url} | Type: {link_type_str} | Context: {determined_context}")
                    csv_written_urls.add(absolute_url)

                if absolute_url in visited_urls:
                    pass
                else:
                    should_queue = False
                    reason_to_skip = None
                    if link_type_str in NON_RECURSIVE_LINK_TYPES:
                        reason_to_skip = f"link type '{link_type_str}' is in NON_RECURSIVE_LINK_TYPES"
                    elif link_type_str.startswith("Activity ("):
                        if not is_current_page_in_target_course_context_flag: # Use the broader context flag here
                            reason_to_skip = (f"activity on a page whose context is not confirmed "
                                              f"for target course '{target_course_id}'")
                        else:
                            should_queue = True
                    else:
                        should_queue = True

                    if should_queue:
                        queue.append((absolute_url, current_url_normalized))
                    elif reason_to_skip:
                        print(f"  Skipping recursion ({reason_to_skip}): {absolute_url}")
                        visited_urls.add(absolute_url)
        
        except requests.exceptions.Timeout:
            print(f"Error: Request timed out for {current_url_normalized}")
            if current_url_normalized not in csv_written_urls:
                writer.writerow([parent_url, current_url_normalized, "N/A (Timeout)", "Error", "N/A"])
                csv_written_urls.add(current_url_normalized)
        except requests.exceptions.HTTPError as e:
            print(f"Error: HTTP error {e.response.status_code} for {current_url_normalized}")
            if current_url_normalized not in csv_written_urls:
                writer.writerow([parent_url, current_url_normalized, f"N/A (HTTP {e.response.status_code})", "Error", "N/A"])
                csv_written_urls.add(current_url_normalized)
        except requests.exceptions.RequestException as e:
            print(f"Error crawling {current_url_normalized}: {e}")
            if current_url_normalized not in csv_written_urls:
                writer.writerow([parent_url, current_url_normalized, "N/A (Request Error)", "Error", "N/A"])
                csv_written_urls.add(current_url_normalized)
        except Exception as e:
            print(f"An unexpected error occurred while processing {current_url_normalized}: {e}")
            if current_url_normalized not in csv_written_urls:
                writer.writerow([parent_url, current_url_normalized, "N/A (Processing Error)", "Error", "N/A"])
                csv_written_urls.add(current_url_normalized)

if __name__ == "__main__":
    try:
        moodle_base_url, username, password, course_id_str = get_moodle_credentials()
    except ValueError as e:
        print(f"Configuration error: {e}")
        exit(1)

    start_url = urljoin(moodle_base_url, f"{COURSE_VIEW_PATH}?id={course_id_str}")

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    if not login_to_moodle(session, moodle_base_url, LOGIN_PATH, username, password):
        print("Exiting due to login failure.")
        exit(1)

    print(f"\nStarting crawl from course page: {start_url} (Target Course ID: {course_id_str})")
    # ... (other startup messages)
    print(f"Context for activity links will be 'Course Page' if on course home, 'Activity Page' if within another activity, or HTML-based otherwise.")


    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Parent URL', 'Link URL', 'Link Text', 'Link Type', 'Location on Page'])
            visited_urls.clear()
            csv_written_urls.clear()
            crawl_page(session, moodle_base_url, start_url, writer, course_id_str)

        print(f"\nCrawling finished.")
        print(f"Processed {len(visited_urls)} unique pages (either crawled or decisioned not to crawl).")
        print(f"Wrote {len(csv_written_urls)} unique URLs to {OUTPUT_CSV_FILE}.")
        print(f"Results saved to {OUTPUT_CSV_FILE}")

    except IOError as e:
        print(f"Error opening or writing to CSV file {OUTPUT_CSV_FILE}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during the main crawl execution: {e}")