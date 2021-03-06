# -*- coding: utf-8 -*-
from base_settings import *

DEBUG = True

TASTYPIE_FULL_DEBUG = True

TEMPLATES[0]['OPTIONS']['debug'] = True

INSTALLED_APPS += [

    'debug_toolbar',

]

MIDDLEWARE_CLASSES = [

    'debug_toolbar.middleware.DebugToolbarMiddleware',

] + MIDDLEWARE_CLASSES



DEBUG_TOOLBAR_PANELS = [

    'debug_toolbar.panels.versions.VersionsPanel',

    'debug_toolbar.panels.timer.TimerPanel',

    'debug_toolbar.panels.settings.SettingsPanel',

    'debug_toolbar.panels.headers.HeadersPanel',

    'debug_toolbar.panels.request.RequestPanel',

    'debug_toolbar.panels.sql.SQLPanel',

    'debug_toolbar.panels.staticfiles.StaticFilesPanel',

    'debug_toolbar.panels.templates.TemplatesPanel',

    'debug_toolbar.panels.cache.CachePanel',

    'debug_toolbar.panels.signals.SignalsPanel',

    'debug_toolbar.panels.logging.LoggingPanel',

    'debug_toolbar.panels.redirects.RedirectsPanel',

    'django_statsd.panel.StatsdPanel',

]



STATSD_CLIENT = 'django_statsd.clients.normal'
