#!/usr/bin/env python
"""Staging version of settings.

USAGE:
  python manage.py runserver --settings=setting.staging_settings

Here we inherit from base settings and just override staging specific items.
"""

from settings import db_config

from settings.base_settings import *


ENVIRON = ENV_STAGING

DEBUG = True


STATIC_ROOT = os.path.join(ROOT_PATH, '../../static_app/')

DATABASES['default'].update(db_config.DEFAULT_STAGING_DATABASE_CONFIG)
DATABASES['readonly'].update(db_config.READONLY_STAGING_DATABASE_CONFIG)

ROOT_URL = ''

ROOT_URL_WITH_SCHEME = 'http://' + ROOT_URL

ALLOWED_HOSTS = ['*']

WSGI_APPLICATION = 'wsgi.staging.application'


CORS_ORIGIN_WHITELIST = (
    '<XXXXX>.<XXXXX>.com',
    'www.<XXXXX>.<XXXXX>.com',
)

TEMPLATES[0]['OPTIONS']['debug'] = True

#--------------- / end / -----------------
