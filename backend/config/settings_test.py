"""
Settings used ONLY for running the test suite.

Why this file exists: the main settings.py hard-requires a running
Postgres + Redis (via docker-compose) because that's what production
needs. Tests shouldn't need real infra spun up just to check that
regex validation logic is correct — that's slow and makes CI harder
to set up. This file inherits everything from settings.py and swaps
only the two infra-dependent pieces:

  - DATABASES  -> in-memory SQLite instead of Postgres
  - CACHES     -> local-memory cache instead of Redis

Run with:
    python manage.py test --settings=config.settings_test
"""
from .settings import *  # noqa: F401,F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}
