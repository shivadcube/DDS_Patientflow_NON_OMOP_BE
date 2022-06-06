import json

from django.conf import settings
from django.conf.urls import url

from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.resources import Resource
from tastypie.utils.urls import trailing_slash

from drugs import mod


class DrugsResource(Resource):

    class Meta:
        resource_name = 'drugs'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/data/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('fetch_ndc_drug_data'),
                name="api_fetch_ndc_drug_data"),
            ]

    def fetch_ndc_drug_data(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        codes = data.get('codes')
        response = mod.ndc_drug_data(codes)
        if response.get("success"):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)
