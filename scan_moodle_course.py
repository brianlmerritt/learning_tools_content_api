import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
import getpass # Keep for full script context
import re
import os
from collections import deque
import ast

# --- Configuration ---
# MOODLE_BASE_URL = os.environ.get("MOODLE_URL", "https://your.moodle.university.edu")
# USERNAME = os.environ.get("MOODLE_USER", "your_username")
# COURSE_ID = os.environ.get("MOODLE_COURSE_ID", "12345")

# --- Constants ---
LOGIN_PATH = "/login/index.php"
COURSE_VIEW_PATH = "/course/view.php"
OUTPUT_CSV_FILE = 'moodle_links.csv'
DELAY_SECONDS = 2
REQUEST_TIMEOUT = 30
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 UniversityLinkCrawler/1.0'

NON_RECURSIVE_LINK_TYPES = {
    "Course Link",
    "Calendar Link",
    "General Internal Link",
    "Gradebook Link",
    "User Profile Link"
}

# --- Global Variables ---
visited_urls = set()
csv_written_urls = set() # MODIFICATION: Replaces printed_as_found_urls, controls CSV writing and initial "Found" message.
moodle_domain = ""

def get_moodle_credentials():
    """Gets Moodle credentials from the user."""
    moodle_base_url = os.getenv("MOODLE_URL", "https://learn.rvc.ac.uk")
    username = os.getenv("MOODLE_USER", None)
    password = os.getenv("MOODLE_PASSWORD", None)
    if username is None or password is None:
        raise ValueError("Please set MOODLE_USER and MOODLE_PASSWORD environment variables.")
    idnumber_list = os.getenv("IDNUMBER_LIST", "[]")
    try:
        parsed_list = ast.literal_eval(idnumber_list)
        if isinstance(parsed_list, list) and parsed_list:
            course_id = str(parsed_list[0])
        else:
            raise ValueError("IDNUMBER_LIST is not a non-empty list.")
    except Exception as e:
        raise ValueError(f"Failed to parse IDNUMBER_LIST: {e}")
    return moodle_base_url, username, password, course_id

def login_to_moodle(session, base_url, login_path, username, password):
    """Logs into Moodle using the provided session and credentials."""
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
        if logintoken:
            print("Found logintoken.")
        else:
            print("Warning: Could not find 'logintoken' field. Login might fail.")

        payload = {'username': username, 'password': password}
        if logintoken:
            payload['logintoken'] = logintoken

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
        if 'response' in locals() and response:
            print("Response status:", response.status_code)
        return False
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return False

def get_link_context(link_tag):
    for parent in link_tag.parents:
        if parent.name == 'div' and parent.get('id') == 'region-main':
            if parent.find('ul', class_='topics') or parent.find('ul', class_='weeks'):
                activity_li = link_tag.find_parent('li', class_=lambda c: c and ('activity' in c.split() or 'resource' in c.split()))
                return "Course Content Area (Activity/Resource)" if activity_li else "Course Content Area (General)"
            if parent.find(class_=re.compile(r'(?<!login)box|generalbox|description')):
                return "Activity/Resource Description"
            return "Course Content Area (Unknown Section)"
        if parent.name == 'div' and 'block' in parent.get('class', []):
            block_header = parent.find(['h2', 'h3', 'h4', 'h5', 'h6'], class_='card-title')
            return f"Block ({block_header.get_text(strip=True) if block_header else 'Unnamed Block'})"
        if parent.name == 'nav': return "Navigation Menu"
        if parent.name == 'header' or parent.get('id') == 'page-header': return "Page Header"
        if parent.name == 'footer' or parent.get('id') == 'page-footer': return "Page Footer"
        if parent.name == 'body': break
    return "Unknown Location"

def classify_link(url):
    path = urlparse(url).path
    if '/mod/' in path:
        parts = path.split('/')
        try:
            mod_index = parts.index('mod')
            return f"Activity ({parts[mod_index + 1]})"
        except (IndexError, ValueError): return "Activity (Unknown Type)"
    elif '/course/view.php' in path: return "Course Link"
    elif '/user/profile.php' in path: return "User Profile Link"
    elif '/grade/report' in path: return "Gradebook Link"
    elif '/calendar/view.php' in path: return "Calendar Link"
    else: return "General Internal Link"

def crawl_page(session, base_url, start_url, writer):
    """Crawls Moodle pages recursively starting from start_url."""
    global csv_written_urls # Ensure we're using the global set

    queue = deque([(start_url, "Initial Entry")])

    while queue:
        current_url, parent_url = queue.popleft()
        current_url_normalized = current_url.strip().split('#')[0]
        if not current_url_normalized:
            continue

        if current_url_normalized in visited_urls:
            continue

        print(f"Crawling: {current_url_normalized} (from: {parent_url})")
        visited_urls.add(current_url_normalized)

        parsed_current = urlparse(current_url_normalized)
        if parsed_current.scheme not in ['http', 'https'] or parsed_current.netloc != moodle_domain:
            print(f"Skipping non-HTTP or external URL: {current_url_normalized}")
            continue

        try:
            time.sleep(DELAY_SECONDS)
            response = session.get(current_url_normalized, timeout=REQUEST_TIMEOUT, headers={'User-Agent': USER_AGENT})
            response.raise_for_status()

            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                print(f"Skipping non-HTML content at {current_url_normalized} (Type: {content_type})")
                # MODIFICATION: Check if this non-HTML link should be added to CSV (once)
                if current_url_normalized not in csv_written_urls:
                    writer.writerow([parent_url, current_url_normalized, "N/A (Non-HTML)", "Non-HTML Resource", "N/A"])
                    csv_written_urls.add(current_url_normalized)
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)

            for link_tag in links:
                href = link_tag['href'].strip()
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue

                absolute_url = urljoin(current_url_normalized, href).split('#')[0]
                link_text = link_tag.get_text(strip=True)

                parsed_absolute = urlparse(absolute_url)
                if parsed_absolute.scheme not in ['http', 'https'] or parsed_absolute.netloc != moodle_domain:
                    # MODIFICATION: Write external links to CSV only once
                    if absolute_url not in csv_written_urls:
                        writer.writerow([current_url_normalized, absolute_url, link_text, "External Link", "N/A"])
                        csv_written_urls.add(absolute_url)
                    continue

                if 'logout' in absolute_url.lower():
                    # MODIFICATION: Write logout links to CSV only once
                    if absolute_url not in csv_written_urls:
                        writer.writerow([current_url_normalized, absolute_url, link_text, "Logout Link", "N/A"])
                        csv_written_urls.add(absolute_url)
                    visited_urls.add(absolute_url) # Mark as "handled" to prevent queuing
                    continue

                link_type = classify_link(absolute_url)
                context = get_link_context(link_tag)

                # --- MODIFICATION: Write to CSV and print "Found" message only for new URLs ---
                if absolute_url not in csv_written_urls:
                    writer.writerow([current_url_normalized, absolute_url, link_text, link_type, context])
                    print(f"  Found internal link (added to CSV): {absolute_url} | Type: {link_type} | Context: {context}")
                    csv_written_urls.add(absolute_url)
                # --- END MODIFICATION ---

                # Logic to decide whether to queue or mark as visited (if non-recursive and new for processing)
                if absolute_url in visited_urls:
                    # Already visited (either crawled or decisioned not to crawl further), so skip.
                    pass
                else:
                    # This is a new URL for crawling consideration (not in visited_urls).
                    if link_type not in NON_RECURSIVE_LINK_TYPES:
                        queue.append((absolute_url, current_url_normalized))
                    else:
                        # Non-recursive type, and not yet in visited_urls.
                        # This "Skipping recursion" message is printed only once for this URL
                        # because we add it to visited_urls immediately after.
                        print(f"  Skipping recursion for non-recursive type '{link_type}': {absolute_url}")
                        visited_urls.add(absolute_url) # Mark as "handled" for recursion purposes

        except requests.exceptions.Timeout:
            print(f"Error: Request timed out while crawling {current_url_normalized}")
            if current_url_normalized not in csv_written_urls: # Log error entry if URL itself is new to CSV
                writer.writerow([parent_url, current_url_normalized, "N/A (Timeout)", "Error", "N/A"])
                csv_written_urls.add(current_url_normalized)
        except requests.exceptions.HTTPError as e:
            print(f"Error: HTTP error {e.response.status_code} while crawling {current_url_normalized}")
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
        moodle_base_url, username, password, course_id = get_moodle_credentials()
    except ValueError as e:
        print(f"Configuration error: {e}")
        exit(1)

    start_url = urljoin(moodle_base_url, f"{COURSE_VIEW_PATH}?id={course_id}")

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    if not login_to_moodle(session, moodle_base_url, LOGIN_PATH, username, password):
        print("Exiting due to login failure.")
        exit(1)

    print(f"\nStarting crawl from course page: {start_url}")
    print(f"Output will be saved to: {OUTPUT_CSV_FILE}")
    print(f"Each unique link URL will be written to the CSV only once.")
    print(f"Links with types {NON_RECURSIVE_LINK_TYPES} will not be crawled recursively.")


    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Parent URL', 'Link URL', 'Link Text', 'Link Type', 'Location on Page'])
            # Reset global sets for multiple runs in the same Python session (if any)
            visited_urls.clear()
            csv_written_urls.clear() # MODIFICATION: Clear the new set as well
            crawl_page(session, moodle_base_url, start_url, writer)

        print(f"\nCrawling finished.")
        print(f"Processed {len(visited_urls)} unique pages (either crawled or decisioned not to crawl).")
        print(f"Wrote {len(csv_written_urls)} unique URLs to {OUTPUT_CSV_FILE}.")
        print(f"Results saved to {OUTPUT_CSV_FILE}")

    except IOError as e:
        print(f"Error opening or writing to CSV file {OUTPUT_CSV_FILE}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during the main crawl execution: {e}")