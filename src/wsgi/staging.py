import djcelery
import os
from os.path import abspath, dirname, join
import sys

from django.core.wsgi import get_wsgi_application

djcelery.setup_loader()

ROOT_PATH = abspath(join(dirname(__file__), '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings.staging_settings'

application = get_wsgi_application()
