"""Local version of settings.
USAGE:
  python manage.py runserver --settings=setting.local_settings
Here we inherit from test settings and just override local specific items.
"""
import os
import logging

from settings import db_config
from settings.base_settings import *

USE_X_FORWARDED_HOST = True
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

DEBUG = False
TASTYPIE_FULL_DEBUG = False

LOGGING = {
    'version': 1,
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'propagate': True,
        },
        'django.request': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'DEBUG',
        },

    },
}


DATABASES['default'].update(db_config.DEFAULT_PROD_DATABASE_CONFIG)
DATABASES['readonly'].update(db_config.READONLY_PROD_DATABASE_CONFIG)
# DATABASES['staging'] = db_config.DEFAULT_LOCAL_DATABASE_CONFIG
# # We use the same database as LOCAL_STAGING as it is only for testing purpose
# DATABASES['prod'] = db_config.DEFAULT_LOCAL_DATABASE_CONFIG

#--------------- CACHING -----------------
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-bar'
    },
    'locmem': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'lm'
    }
}
#-------------- / end / ------------------

ALLOWED_HOSTS = ['*']

MIDDLEWARE_CLASSES += (
    'debug_toolbar.middleware.DebugToolbarMiddleware',
)

TEMPLATES[0]['OPTIONS']['debug'] = False
TEMPLATES[0]['OPTIONS']['debug'] = False

INSTALLED_APPS += (
    'debug_toolbar',
)

WSGI_APPLICATION = 'wsgi.prod.application'

if len(sys.argv) > 1 and sys.argv[1] == 'test':
    logging.disable(logging.WARNING)

RUNNING_TESTS = True


AWS_COMPETETION_BUCKET = ''

SECURE_SSL_REDIRECT = True
CSRF_COOKIE_SECURE = True
SESSION_COOKIE_SECURE = True
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTOCOL', 'https')
