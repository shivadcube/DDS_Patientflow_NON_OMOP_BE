"""DDS3_ADMIN URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
#!/usr/bin/python

import re
import sys

import django

from Crypto import Random
from django.conf import settings
from django.conf.urls import include
from django.conf.urls import url
from django.contrib import admin
from django.views.generic import TemplateView
from django.views.generic import RedirectView

from tastypie.api import NamespacedApi

from attributes.api import AttributesResource, AnalysisResource
from cohort.api import CohortResource
from drugs.api import DrugsResource
from lot.api import LotResource
from projects.api import ProjectsResource
from services.livy.api import LivyResource
from accounts.api import AccountResource
from comparison_tab.api import ComparisonTabResource

admin.autodiscover()

v1_api = NamespacedApi(api_name='v1', urlconf_namespace='namespace')
v1_api.register(AttributesResource())
v1_api.register(CohortResource())
v1_api.register(DrugsResource())
v1_api.register(LivyResource())
v1_api.register(LotResource())
v1_api.register(ProjectsResource())
v1_api.register(AccountResource())
v1_api.register(ComparisonTabResource())
v1_api.register(AnalysisResource())

urlpatterns = [

    # admin urls
    url(r'^dds3/patientflow/admin/', include(admin.site.urls)),
    url(r'^dds3/patientflow/admin/doc/',
        include('django.contrib.admindocs.urls')),
    # api v1 urls
    url(r'^api/', include(v1_api.urls)),

]

Random.atfork()


if settings.DEBUG or 'test' in sys.argv:
    import debug_toolbar
    static_url = re.escape(settings.STATIC_URL.lstrip('/'))
    urlpatterns += [
        url(r'^%s(?P<path>.*)$' % static_url, django.views.static.serve, {
            'document_root': settings.STATIC_ROOT,
        }),
    ]
    media_url = re.escape(settings.MEDIA_URL.lstrip('/'))
    urlpatterns += [
        url(r'^%s(?P<path>.*)$' % media_url, django.views.static.serve, {
            'document_root': settings.MEDIA_ROOT,
        }),
    ]
    
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]
