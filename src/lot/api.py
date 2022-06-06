import json

from django.conf import settings
from django.conf.urls import url

from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.resources import Resource
from tastypie.utils.urls import trailing_slash

from lot import mod


class LotResource(Resource):

    class Meta:
        resource_name = 'lot'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/attributes/mappings%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('attributes_mapping'),
                name="api_lot_attributes"),
            url(r"^(?P<resource_name>%s)/deep/dive%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('deep_dive_analysis'),
                name="api_deep_dive_analysis"),
            url(r"^(?P<resource_name>%s)/regimen/view%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('view_regimen'),
                name="api_view_regimen"),
            url(r"^(?P<resource_name>%s)/deep-dive%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('deep_dive_acceptance_analysis'),
                name="api_deep_dive_acceptance_analysis"),
            url(r"^(?P<resource_name>%s)/regimen-stackbar%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('deep_dive_analysis_stack_bar'),
                name="api_deep_dive_analysis_stack_bar"),
            url(r"^(?P<resource_name>%s)/regimen-mono-combo%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('deep_dive_analysis_mono_combo'),
                name="api_deep_dive_analysis_mono_combo"),
            url(r"^(?P<resource_name>%s)/regimen-avg-days%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('deep_dive_analysis_average_days'),
                name="api_deep_dive_analysis_average_days"),
        ]

    def attributes_mapping(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.attributes_mapping(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def deep_dive_analysis(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.deep_dive_analysis(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def view_regimen(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.view_regimen(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def deep_dive_acceptance_analysis(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.deep_dive_acceptance_analysis(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def deep_dive_analysis_stack_bar(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.deep_dive_analysis_stack_bar(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def deep_dive_analysis_mono_combo(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.deep_dive_analysis_mono_combo(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def deep_dive_analysis_average_days(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.deep_dive_analysis_average_days(request, data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)
