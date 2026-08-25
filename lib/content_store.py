"""SQLite content store - one database file per corpus, replacing the
per-course CSV trees (ruled 2026-08-25; Brian is the sole consumer of the
extraction routines).

Shape: a `course` manifest table (one row per course, with contents_hash and
first/last extraction stamps - the "most recent update" flag), plus one table
per former CSV kind whose COLUMN NAMES ARE THE OLD CSV HEADERS, so readers
migrate mechanically from load_csv(...) to store.rows(table, course_id).
Tables are created on first sight of their DataFrame and widened with ALTER
TABLE if a later course carries extra columns; list/dict cells are stored as
JSON strings (the CSVs stringified them too).

Refresh model: replace-per-course in one transaction. A course whose
core_course_get_contents payload hashes the same as last time is skipped by
the fetcher (contents_hash covers names, structure, visibility and the file
timestamps inside module contents), so unchanged courses cost a handful of
list calls and no content fetches.
"""
import json
import os
import sqlite3
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# Fields of the ws course payload kept on the manifest row.
COURSE_FIELDS = ['idnumber', 'shortname', 'fullname', 'categoryid', 'categoryname',
                 'format', 'visible', 'startdate', 'enddate', 'summary', 'timemodified']


def _cell(value):
    """A SQLite-storable value: scalars pass, containers become JSON."""
    if value is None:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_, bool)):
        return int(value)
    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, default=str)
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


class content_store:
    def __init__(self, db_path):
        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS course (
                course_id INTEGER PRIMARY KEY,
                idnumber TEXT, shortname TEXT, fullname TEXT,
                categoryid INTEGER, categoryname TEXT, format TEXT,
                visible INTEGER, startdate INTEGER, enddate INTEGER,
                summary TEXT, timemodified INTEGER,
                contents_hash TEXT,
                first_extracted_at TEXT, last_extracted_at TEXT)
        """)
        self.connection.commit()

    def unchanged(self, course_id, contents_hash):
        row = self.connection.execute(
            'SELECT contents_hash FROM course WHERE course_id = ?', (int(course_id),)).fetchone()
        return row is not None and row['contents_hash'] == contents_hash and contents_hash

    def replace_course(self, course, contents_hash, tables):
        """Upsert the manifest row and replace every table's rows for this
        course, in one transaction. tables = {name: DataFrame}."""
        course_id = int(course['id'])
        now = datetime.now(timezone.utc).isoformat(timespec='seconds')
        with self.connection:
            values = [_cell(course.get(field)) for field in COURSE_FIELDS]
            self.connection.execute(f"""
                INSERT INTO course (course_id, {', '.join(COURSE_FIELDS)},
                                    contents_hash, first_extracted_at, last_extracted_at)
                VALUES (?{', ?' * len(COURSE_FIELDS)}, ?, ?, ?)
                ON CONFLICT(course_id) DO UPDATE SET
                    {', '.join(f'{f} = excluded.{f}' for f in COURSE_FIELDS)},
                    contents_hash = excluded.contents_hash,
                    last_extracted_at = excluded.last_extracted_at
            """, [course_id] + values + [contents_hash, now, now])
            for name, frame in tables.items():
                self._replace_table(name, course_id, frame)

    def _replace_table(self, name, course_id, frame):
        if not isinstance(frame, pd.DataFrame):
            try:
                frame = pd.DataFrame(frame if frame is not None else [])
            except ValueError:
                # Scalar placeholders (the forum handler yields 0 for
                # forum-less courses) mean "no rows".
                frame = pd.DataFrame()
        frame = frame.copy()
        frame['course_id'] = course_id
        columns = [str(c) for c in frame.columns]
        existing = self._columns(name)
        if existing is None:
            quoted = ', '.join(f'"{c}"' for c in columns)
            self.connection.execute(f'CREATE TABLE "{name}" ({quoted})')
        else:
            for column in columns:
                if column not in existing:
                    self.connection.execute(f'ALTER TABLE "{name}" ADD COLUMN "{column}"')
        self.connection.execute(f'DELETE FROM "{name}" WHERE course_id = ?', (course_id,))
        if frame.empty:
            return
        placeholders = ', '.join('?' for _ in columns)
        quoted = ', '.join(f'"{c}"' for c in columns)
        sql = f'INSERT INTO "{name}" ({quoted}) VALUES ({placeholders})'
        rows = [[_cell(v) for v in record] for record in frame.itertuples(index=False, name=None)]
        self.connection.executemany(sql, rows)

    def _columns(self, name):
        rows = self.connection.execute(
            "SELECT name FROM pragma_table_info(?)", (name,)).fetchall()
        return [r['name'] for r in rows] or None

    def courses(self):
        """Manifest rows as dicts, idnumber order."""
        rows = self.connection.execute('SELECT * FROM course ORDER BY idnumber').fetchall()
        return [dict(r) for r in rows]

    def course(self, course_id):
        row = self.connection.execute(
            'SELECT * FROM course WHERE course_id = ?', (int(course_id),)).fetchone()
        return dict(row) if row else None

    def rows(self, table, course_id=None):
        """A table's rows as dicts (same keys as the old CSV headers).
        Missing table = empty list, matching load_csv on a missing file."""
        if self._columns(table) is None:
            return []
        if course_id is None:
            cursor = self.connection.execute(f'SELECT * FROM "{table}"')
        else:
            cursor = self.connection.execute(
                f'SELECT * FROM "{table}" WHERE course_id = ?', (int(course_id),))
        return [dict(r) for r in cursor.fetchall()]

    def close(self):
        self.connection.close()
