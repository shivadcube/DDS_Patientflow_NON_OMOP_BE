#!/usr/bin/env python
"""Production readonly version of settings.
USAGE:
  python manage.py runserver --settings=setting.prod_readonly_settings

Here we inherit from prod settings and just override prod default database user to readonly user.
"""


from prod_settings import *
import db_config 


DATABASES['default'].update(db_config.READONLY_PROD_DATABASE_CONFIG)
