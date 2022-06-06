import json
import os

from django.conf import settings
from django.conf.urls import url
from django.http import HttpResponse

from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.resources import Resource
from tastypie.utils.urls import trailing_slash

from cohort import mod
from cohort import constants

import requests
import environ
env = environ.Env()
environ.Env.read_env()


class CohortResource(Resource):
    class Meta:
        resource_name = 'cohort'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/import/list/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('cohort_list'),
                name="api_cohort_list"),
            url(r"^(?P<resource_name>%s)/upload/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('upload_cohort'),
                name="api_upload_cohort"),
            url(r"^(?P<resource_name>%s)/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('download_sample_cohort'),
                name="api_download_sample_cohort"),
            url(r"^(?P<resource_name>%s)/import/summary%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('import_cohort'),
                name="api_import_cohort"),
        ]

    def upload_cohort(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        file = request.FILES["File"]
        session_id = request.POST.get("session_id")
        analysis_id = request.POST.get("analysis_id")
        response = mod.upload_cohort(request, file,
                                     session_id,
                                     analysis_id)
        if response.get("success"):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def download_sample_cohort(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        file_path = settings.ROOT_PATH + constants.COHORT_FILE_PATH
        if os.path.exists(file_path):
            with open(file_path, 'rb') as fh:
                response = HttpResponse(fh.read(), content_type='text/csv')
                response['Content-Disposition'] = 'inline; filename=' + \
                                                  os.path.basename(file_path)
                return response
        return self.error_response(request, HttpBadRequest)

    def import_cohort(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        cohort_id = request.GET.get('cohort_id')
        session_id = request.GET.get('session_id')
        analysis_id = request.GET.get('analysis_id')
        history_id = request.GET.get('history_id')
        env_type = request.GET.get('env_type')
        response = mod.import_cohort_summary(cohort_id,
                                             session_id,
                                             analysis_id,
                                             history_id, env_type)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def cohort_list(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        try:
            url = env('COHORT_URL')
            my_headers = {'Authorization': 'Token {}'.format(env('OPEN_TOKEN'))}
            res = requests.get(url=url, headers=my_headers)
            if res.status_code != 200:
                return self.error_response(request, res.text, HttpBadRequest)
            response = json.loads(res.text)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)
        except Exception as e:
            return self.error_response(request, {'error': str(e)}, HttpBadRequest)
