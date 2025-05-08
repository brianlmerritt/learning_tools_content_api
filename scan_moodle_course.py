import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
import getpass
import re
import os
from collections import deque

# --- Configuration ---
# Consider using environment variables or a config file for sensitive data
# MOODLE_BASE_URL = os.environ.get("MOODLE_URL", "https://your.moodle.university.edu") # Example
# USERNAME = os.environ.get("MOODLE_USER", "your_username") # Example
# COURSE_ID = os.environ.get("MOODLE_COURSE_ID", "12345") # Example course ID

# --- Constants ---
LOGIN_PATH = "/login/index.php"
COURSE_VIEW_PATH = "/course/view.php"
OUTPUT_CSV_FILE = 'moodle_links.csv'
DELAY_SECONDS = 2 # Time to wait between requests to be polite
REQUEST_TIMEOUT = 30 # Seconds to wait for a response
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 UniversityLinkCrawler/1.0'

# --- Global Variables ---
visited_urls = set()
moodle_domain = "" # Will be set after getting base URL

def get_moodle_credentials():
    """Gets Moodle credentials from the user."""
    moodle_base_url = input("Enter the base URL of your Moodle instance (e.g., https://moodle.example.com): ").strip().rstrip('/')
    username = input("Enter your Moodle username: ").strip()
    password = getpass.getpass("Enter your Moodle password: ")
    course_id = input("Enter the Moodle Course ID to start crawling: ").strip()
    return moodle_base_url, username, password, course_id

def login_to_moodle(session, base_url, login_path, username, password):
    """Logs into Moodle using the provided session and credentials."""
    global moodle_domain
    parsed_base_url = urlparse(base_url)
    moodle_domain = parsed_base_url.netloc

    login_url = urljoin(base_url, login_path)
    print(f"Attempting to access login page: {login_url}")

    try:
        # 1. Get the login page to find hidden tokens (like logintoken)
        response = session.get(login_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Raise an exception for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the login token (common name, might need adjustment)
        logintoken_tag = soup.find('input', {'name': 'logintoken'})
        if not logintoken_tag or not logintoken_tag.get('value'):
            print("Warning: Could not find 'logintoken' field. Login might fail.")
            logintoken = None
        else:
            logintoken = logintoken_tag['value']
            print("Found logintoken.")

        # 2. Prepare login data payload
        payload = {
            'username': username,
            'password': password,
            # Add other form fields if necessary (inspect the login form)
        }
        if logintoken:
            payload['logintoken'] = logintoken

        print(f"Attempting to POST login credentials to {login_url}")
        # 3. Send POST request to log in
        response = session.post(login_url, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        # 4. Check if login was successful
        #    This check is heuristic. Moodle might redirect to dashboard or user profile.
        #    Check if the response URL is different from the login URL or
        #    if specific text indicating successful login appears (e.g., username)
        if response.url == login_url or "login/index.php" in response.url:
             # Sometimes Moodle redirects back with an error parameter
             soup_check = BeautifulSoup(response.text, 'html.parser')
             if soup_check.find(class_='loginerrors') or soup_check.find(id='loginerrormessage'):
                 print("Login Failed! Check username/password. Error message found on page.")
                 return False
             else:
                 # Might still be on login page but without error, could be unexpected state
                 print("Warning: Still on login page after POST, but no explicit error found. Assuming login failed.")
                 return False


        print("Login Successful! (heuristic check passed)")
        # Optional: Verify by trying to access a page requiring login
        # test_url = urljoin(base_url, "/my/") # Example dashboard URL
        # test_resp = session.get(test_url, timeout=REQUEST_TIMEOUT)
        # if "login/index.php" in test_resp.url:
        #     print("Login verification failed: redirected back to login page.")
        #     return False
        # print("Login verification successful.")
        return True

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out while trying to connect to {login_url}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Error during login process: {e}")
        # Print response text if available for debugging
        if 'response' in locals() and response:
            print("Response status:", response.status_code)
            # print("Response text (first 500 chars):", response.text[:500]) # Careful with printing potentially large pages
        return False
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return False


def get_link_context(link_tag):
    """
    Tries to determine where on the page the link was found.
    **THIS FUNCTION REQUIRES CUSTOMIZATION** based on your Moodle theme's HTML.
    Inspect your Moodle page source to find reliable parent elements/classes.
    """
    # Search upwards for characteristic parent elements
    for parent in link_tag.parents:
        if parent.name == 'div' and parent.get('id') == 'region-main':
            # Check if it's directly within the main course content list
            if parent.find('ul', class_='topics') or parent.find('ul', class_='weeks'):
                 # Check if it's inside an activity list item
                 activity_li = link_tag.find_parent('li', class_=lambda c: c and ('activity' in c.split() or 'resource' in c.split()))
                 if activity_li:
                     return "Course Content Area (Activity/Resource)"
                 else:
                     return "Course Content Area (General)"
            # Check if inside an activity description/page itself
            if parent.find(class_=re.compile(r'(?<!login)box|generalbox|description')): # e.g., forum post, assignment description
                 return "Activity/Resource Description"
            return "Course Content Area (Unknown Section)"

        if parent.name == 'div' and 'block' in parent.get('class', []):
            block_header = parent.find(['h2', 'h3', 'h4', 'h5', 'h6'], class_='card-title')
            block_title = block_header.get_text(strip=True) if block_header else "Unnamed Block"
            return f"Block ({block_title})"

        if parent.name == 'nav':
            return "Navigation Menu"

        if parent.name == 'header' or parent.get('id') == 'page-header':
            return "Page Header"

        if parent.name == 'footer' or parent.get('id') == 'page-footer':
            return "Page Footer"

        # Stop searching too high up
        if parent.name == 'body':
            break

    # Default if no specific context found
    return "Unknown Location"

def classify_link(url):
    """Classifies the Moodle link based on its URL structure."""
    path = urlparse(url).path
    if '/mod/' in path:
        parts = path.split('/')
        try:
            mod_index = parts.index('mod')
            activity_type = parts[mod_index + 1]
            return f"Activity ({activity_type})"
        except (IndexError, ValueError):
            return "Activity (Unknown Type)"
    elif '/course/view.php' in path:
        return "Course Link"
    elif '/user/profile.php' in path:
        return "User Profile Link"
    elif '/grade/report' in path:
        return "Gradebook Link"
    elif '/calendar/view.php' in path:
        return "Calendar Link"
    # Add more classifications as needed
    else:
        return "General Internal Link"

def crawl_page(session, base_url, start_url, writer):
    """Crawls Moodle pages recursively starting from start_url."""
    queue = deque([(start_url, "Initial Entry")]) # Queue stores (url_to_crawl, parent_url)

    while queue:
        current_url, parent_url = queue.popleft()

        if current_url in visited_urls:
            continue

        # Basic URL cleanup/normalization
        current_url = current_url.strip().split('#')[0] # Remove fragments
        if not current_url: continue

        # Ensure we only crawl HTTP/HTTPS URLs within the same Moodle domain
        parsed_current = urlparse(current_url)
        if parsed_current.scheme not in ['http', 'https'] or parsed_current.netloc != moodle_domain:
             print(f"Skipping non-HTTP or external URL: {current_url}")
             continue

        # Check again after cleanup/normalization
        if current_url in visited_urls:
            continue

        print(f"Crawling: {current_url} (from: {parent_url})")
        visited_urls.add(current_url)

        try:
            time.sleep(DELAY_SECONDS) # Be polite!
            response = session.get(current_url, timeout=REQUEST_TIMEOUT, headers={'User-Agent': USER_AGENT})
            response.raise_for_status()

            # Check content type - only parse HTML
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                print(f"Skipping non-HTML content at {current_url} (Type: {content_type})")
                # Record the link existence but don't parse/follow
                writer.writerow([parent_url, current_url, "N/A (Non-HTML)", "Non-HTML Resource", "N/A"])
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all links on the page
            links = soup.find_all('a', href=True)

            for link_tag in links:
                href = link_tag['href'].strip()
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue # Skip empty, fragment, or javascript links

                # Construct absolute URL
                absolute_url = urljoin(current_url, href).split('#')[0] # Use current_url as base

                # Get link text
                link_text = link_tag.get_text(strip=True)

                # Filter: Only process links within the same Moodle domain
                parsed_absolute = urlparse(absolute_url)
                if parsed_absolute.scheme not in ['http', 'https'] or parsed_absolute.netloc != moodle_domain:
                    # Optional: Log external links if desired
                    # print(f"  Found external link: {absolute_url}")
                    writer.writerow([current_url, absolute_url, link_text, "External Link", "N/A"])
                    continue

                # Filter: Avoid logout links
                if 'logout' in absolute_url.lower():
                     writer.writerow([current_url, absolute_url, link_text, "Logout Link", "N/A"])
                     continue

                # Classify and get context
                link_type = classify_link(absolute_url)
                context = get_link_context(link_tag)

                # Store relationship
                print(f"  Found internal link: {absolute_url} | Type: {link_type} | Context: {context}")
                writer.writerow([current_url, absolute_url, link_text, link_type, context])

                # Add valid internal link to the queue if not visited
                if absolute_url not in visited_urls:
                    queue.append((absolute_url, current_url))

        except requests.exceptions.Timeout:
             print(f"Error: Request timed out while crawling {current_url}")
             writer.writerow([parent_url, current_url, "N/A (Timeout)", "Error", "N/A"])
        except requests.exceptions.HTTPError as e:
             print(f"Error: HTTP error {e.response.status_code} while crawling {current_url}")
             writer.writerow([parent_url, current_url, f"N/A (HTTP {e.response.status_code})", "Error", "N/A"])
        except requests.exceptions.RequestException as e:
            print(f"Error crawling {current_url}: {e}")
            writer.writerow([parent_url, current_url, "N/A (Request Error)", "Error", "N/A"])
        except Exception as e:
            print(f"An unexpected error occurred while processing {current_url}: {e}")
            writer.writerow([parent_url, current_url, "N/A (Processing Error)", "Error", "N/A"])


# --- Main Execution ---
if __name__ == "__main__":
    moodle_base_url, username, password, course_id = get_moodle_credentials()
    start_url = urljoin(moodle_base_url, f"{COURSE_VIEW_PATH}?id={course_id}")

    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT}) # Set user agent

    if not login_to_moodle(session, moodle_base_url, LOGIN_PATH, username, password):
        print("Exiting due to login failure.")
        exit(1)

    print(f"\nStarting crawl from course page: {start_url}")
    print(f"Output will be saved to: {OUTPUT_CSV_FILE}")

    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header row
            writer.writerow(['Parent URL', 'Link URL', 'Link Text', 'Link Type', 'Location on Page'])

            # Start the crawl
            crawl_page(session, moodle_base_url, start_url, writer)

        print(f"\nCrawling finished. Visited {len(visited_urls)} unique internal pages.")
        print(f"Results saved to {OUTPUT_CSV_FILE}")

    except IOError as e:
        print(f"Error opening or writing to CSV file {OUTPUT_CSV_FILE}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during the main crawl execution: {e}")