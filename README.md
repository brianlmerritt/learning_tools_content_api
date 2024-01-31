# Moodle Content API

This program is designed to read content from a Moodle system, sanitize it, and save it as text or publish it to a LLM vector store like faiss.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

To install the Moodle Content API, follow these steps:

1. Clone the repository: `git clone https://github.com/brianlmerritt/moodle-content-api.git`
1. Install the required dependencies: `pip install -r requirements.txt`

You also need to setup Web Services (REST) and generate a user token

If that user doesn't have full view all courses & categories, restrict requests to course by course or search of courses by pattern instead of find all courses.

To use the Moodle Content API, you need to provide the necessary configuration settings. Update the `.env` file with your Moodle web token and other required parameters - use the .env_example to help.

## Usage

Once the configuration is set up, you can run the program using the following command:

`python3 get_moodle_courses_data.py`

Files are stored in `course_data`

## TODO ##

1. Finish extract data from Moodle for blocks, books, pages, files, labels, forums (to start)
1. Save course data files to import in CourseID_#_IDNUMBER directory
1. Extract study map function if applicable (at RVC it is strand map)
1. Build text import routines to save sanitised data (plain text & .md format?) with meta data from course, section, module, and study map if applicable
1. Set up contributing possibility
1. Add other content types (lesson, quiz?)
1. Add LTI & other content via Selenium?
1. Add lecture capture

## Contributing ##

Coming soon









