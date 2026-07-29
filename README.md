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

## Strand mapping (build_modules_with_strands.py)

After `get_moodle_courses_data.py` has populated `course_data/`, run:

`python3 build_modules_with_strands.py`            # all courses
`python3 build_modules_with_strands.py <folder>`   # one course

Outputs, per course and combined:

- `{course}_modules_with_strands.csv` - one row per content item (books,
  pages, labels, URLs, resources, folders, forums) with section, strand,
  sub-strand, week and `in_strand_map`.
- `course_data/all_modules_with_strands.csv` - all courses combined.
- `course_data/all_courses_with_strands.csv` - one row per course:
  course id/fullname/shortname/category, `course_strand` (see below),
  `has_strand_map`, item counts. The course-level companion to the
  per-item CSV (together they prefigure the planned two-table
  moodle_content design: course table + content table).

Strand assignment, in priority order:

1. **Strand Map page links** - courses with a "Strand Map" page (the
   BVetMed hub estate) resolve items via the map, `in_strand_map = True`.
2. **Section inheritance** - items sharing a section with strand-mapped
   content inherit the section's strand.
3. **Strand-course any-term rule** - a course whose idnumber matches the
   SRS strand-module dialect (`1VETS01_A_Y_202627`-style, or the UBVETMD
   PVP course) and whose fullname resolves to a known strand IS a strand
   course (`course_strand` is set). Inside it, any full medical strand
   term in an item name or label text flags the item - no adjacency
   needed. Short abbreviations (ah, cs, pos...) and the word "skin" still
   require rule 4's adjacency; both restrictions are deliberate
   false-positive guards.
4. **Term+strand adjacency rule** (all courses) - a strand term
   immediately preceded or followed by the word "strand"/"strands"
   ("Alimentary Strand", "the POS strand", "Strand: Alimentary").
5. Otherwise the `strand` column falls back to the content type.

The vocabulary (`STRAND_TERMS`) is vendored from moodle-local_curricmap
`matcher.php::default_rules()` synonyms - keep the two in sync. Same
deliberate exclusions as the plugin: `end`, `nma`.

Note on file resources: `mod_resource`/`mod_folder` contents are files
with no html sibling; for those, `is_used` means "module visible" (the
file IS the resource), whereas book/page files require their filename to
appear in the prose. Earlier extracts silently dropped files-only groups
entirely - re-extract any `course_data` captured before 2026-07-27.

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









