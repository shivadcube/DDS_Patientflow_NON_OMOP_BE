#!/usr/bin/env python

"""Staging readonly version of settings.

USAGE:
  python manage.py runserver --settings=setting.staging_readonly_settings

Here we inherit from staging settings and just override staging default database user to readonly user.
"""

from staging_settings import *
import db_config 

DATABASES['default'].update(db_config.READONLY_STAGING_DATABASE_CONFIG)
