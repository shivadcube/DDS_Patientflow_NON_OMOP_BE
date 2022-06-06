import json

from django.conf import settings
from django.conf.urls import url

from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.resources import Resource
from tastypie.utils.urls import trailing_slash

from projects import mod


class ProjectsResource(Resource):
    class Meta:
        resource_name = 'projects'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/(?P<project_id>[0-9]*)%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('projects'),
                name="api_projects"),
            url(r"^(?P<resource_name>%s)/validation%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('name_validation'),
                name="api_name_validation"),
            url(r"^(?P<resource_name>%s)/analysis/sheet%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('analysis_summary_sheet'),
                name="api_analysis_summary_sheet"),
            url(r"^(?P<resource_name>%s)/analysis/period%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('analysis_period'),
                name="api_analysis_period"),
            url(r"^(?P<resource_name>%s)/job-status%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('fetch_job_status'),
                name="api_fetch_job_status"),
        ]

    def projects(self, request, project_id=None, *args, **kwargs):
        self.method_check(request, allowed=['get', 'post', 'put', 'delete'])
        self.is_authenticated(request)
        user = request.user
        if request.method == 'GET':
            action = request.GET.get('action', 'view')
            response = mod.projects_list(project_id, action)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

        if request.method == 'POST':
            data = json.loads(request.body)
            response = mod.create_project(request, user, data)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

        if request.method == 'PUT':
            data = json.loads(request.body.decode('utf-8'))
            response = mod.update_project(request, data)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

        if request.method == 'DELETE':
            response = mod.delete_projects(project_id)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

    def name_validation(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        name = request.GET.get('name')
        response = mod.validate_project_name(name)
        if response.get("success"):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def analysis_summary_sheet(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.analysis_summary_sheet(request, data)
        if response.get("success"):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def analysis_period(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.analysis_period(request, data)
        if response.get("success"):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def fetch_job_status(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        project_id = request.GET.get('project_id')
        analysis_id = request.GET.get('analysis_id')
        response = mod.fetch_job_status(project_id, analysis_id)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)
