"""Runtime configuration from config.yaml (repo root).

Non-secret run settings live here; credentials and server URLs stay in .env.
Loaded once and cached; tests may pre-seed _config before first use.
"""
import os

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_config = None


def config():
    global _config
    if _config is None:
        path = os.path.join(REPO_ROOT, 'config.yaml')
        with open(path, encoding='utf-8') as fh:
            _config = yaml.safe_load(fh) or {}
    return _config


def use_uat():
    return bool(config().get('use_uat', False))


def course_data_dir():
    """Absolute path of the corpus directory (assets, outputs, sqlite)."""
    directory = str(config().get('course_data_dir', 'course_data'))
    if not os.path.isabs(directory):
        directory = os.path.join(REPO_ROOT, directory)
    return directory.rstrip('/')


def sqlite_path():
    return os.path.join(course_data_dir(), 'content.sqlite3')


def idnumber_search():
    return str(config().get('idnumber_search', '') or '')


def idnumber_search_list():
    values = config().get('idnumber_search_list') or []
    return [str(v) for v in values]


def idnumber_list():
    values = config().get('idnumber_list') or []
    return [str(v) for v in values]
