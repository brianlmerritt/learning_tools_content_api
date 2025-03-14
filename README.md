# Learning Tools Content API

This program is designed to read content from a Moodle system, sanitize it, and save it as text or publish it to a LLM vector store.

It works by searching for courses, and then extracting the content of the course either into a .csv file or (todo) a vector store or search engine.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

To install the Moodle Content API, follow these steps:

1. Clone the repository: `git clone https://github.com/brianlmerritt/learning_tools-content-api.git`
1. Install the required dependencies: `pip install -r requirements.txt` # Note you are better to install a virtual env

You also need to setup Web Services (REST) and generate a user token

If that user doesn't have full view all courses & categories, restrict requests to course by course or search of courses by pattern instead of find all courses.

To use the Moodle Content API, you need to provide the necessary configuration settings. Update the `.env` file with your Moodle web token and other required parameters - use the .env_example to help.

## Usage

Once the configuration is set up, you can run the program using the following command:

`python3 get_moodle_courses_data.py`

Files are stored in `course_data`

A helper utility can extract all urls from the activity content.

`python3 extract_urls.py`

## Content extraction is working for Moodle:

- Pages
- Books
- Files (Course level files)
- Folders (To be tested)
- Labels
- Blocks
- URLs
- Forums

## TODO ##

1. Extract data from Moodle for quizzes (to start)
1. Extract study map function if applicable (at RVC it is strand map)
1. Output content to RAG api for indexing, vector database, retrieval
1. Build text import routines to save sanitised data (plain text & .md format?) with meta data from course, section, module, and study map if applicable
1. Set up contributing possibility
1. Add LTI & other content via Selenium?
1. Add lecture capture

## Contributing ##

Coming soon









