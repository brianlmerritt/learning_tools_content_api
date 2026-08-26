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

Minimum access (verified on live, 2026-08-25): a COURSE VIEWER role for the
token's user is enough for this app - it needs to enter course contexts
without enrolment (`moodle/course:view`, plus view-hidden capabilities if
hidden courses matter), not to edit anything. A system-level editing teacher
alone is NOT enough (that archetype lacks `moodle/course:view`), which
presents as "Course or activity not accessible" on every unenrolled course
and an almost-empty full course list.

Configuration is split in two (since 2026-08-25):

- `.env` — credentials and server URLs only (`MOODLE_*`, `UAT_MOODLE_*`); use `.env_example` to help.
- `config.yaml` (committed) — run settings: `use_uat`, `course_data_dir`, and the course filters (`idnumber_search`, `idnumber_search_list`, `idnumber_list`).

## Usage

Once the configuration is set up, you can run the program using the following command:

`python3 get_moodle_courses_data.py`

Extracted content lands in a **sqlite store**: `<course_data_dir>/content.sqlite3`
(since 2026-08-25, replacing the per-course CSV tree). Tables keep the old CSV
column names (`sections`, `books`, `pages`, `labels`, `urls`, `files`,
`folders`, `blocks`, `forums`), plus:

- `course` — one manifest row per course with `contents_hash` and
  `first/last_extracted_at`. A course whose `core_course_get_contents` payload
  hashes the same as last time is **skipped** (no content refetch); pass
  `--force` to re-extract everything.
- `course_modules` — the true all-modules union from `get_contents`,
  subsection cms included (the CSV era discarded this).
- `modules` — keeps its historical (misleading) name: it is the
  `mod_resource_get_resources_by_courses` payload.
- `sections.parent_section_id` — owning parent for Moodle 4.5 delegated
  subsections, empty for plain sections.

`build_staging_content.py` reads the store and still writes
`course_content.csv` / `activity_content.csv` for the curriculum_mapping
loader; cleaned assets still land in `<course_data_dir>/<idnumber>/`.
Delegated-section names are staged parent-prefixed ("Week 1 - 6th Oct › Monday").
It is transitional: the plan (2026-08-25) is for the staging loader to read
`content.sqlite3` directly, after which this script retires.

Still CSV-based (read the pre-sqlite `course_data*` trees, migrate when next
needed): `analyse_course.py`, `extract_urls.py`, `analyze_csv.py`, and the
sofia experiment's `4b_extract_import.py`. NOTE: 4b reads per-course
`{idnumber}_modules_with_strands.csv` files, which are NO LONGER produced -
migrate 4b to read the store's `modules_with_strands` table before its next
use.

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

## Strand mapping (part of get_moodle_courses_data.py since 2026-08-25)

Strand mapping runs at the END of every `get_moodle_courses_data.py` run, so
the sqlite store is complete at fetch time. Two passes over the store: pass 1
collects the strand-map legends across every stored course, pass 2 rebuilds
each course's rows (pure local computation, no API calls - a full rebuild per
run keeps everything consistent with the freshest legends). The logic lives
in `lib/strand_mapping.py` (carried verbatim from the superseded
`build_modules_with_strands.py`, which remains only as the CSV-corpus-era
tool and is not maintained). Note: on learn-uat copies of live courses the
strand-map week table resolves to 0 entries by design - the page HTML links
to LIVE cmids that do not exist in the copy.

Outputs are STORE TABLES ONLY (sqlite-only output, ruled later 2026-08-25 -
no per-course folders or CSVs from the fetch beyond `log_events.csv`):

- `modules_with_strands` - one row per content item (books, pages, labels,
  URLs, resources, folders, forums) with section, strand, sub-strand, week
  and `in_strand_map`. Replaced per course, so single-course reprocessing
  never clobbers the estate.
- `courses_with_strands` - one row per course: course id/fullname/
  shortname/category, `course_strand` (see below), `has_strand_map`, item
  counts. The course-level companion to the per-item table (together they
  prefigure the planned two-table moodle_content design).

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









