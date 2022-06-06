import json
import os

from django.forms import model_to_dict
from django.conf.urls import url
from django.conf import settings
from django.http import HttpResponse
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.utils.urls import trailing_slash
from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.resources import Resource

from attributes import mod


class AttributesResource(Resource):
    class Meta:
        resource_name = 'attributes'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('fetch_attributes'),
                name='api_fetch_attributes'),
            url(r"^(?P<resource_name>%s)/mappings%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('attributes_mapping'),
                name='api_attributes_mapping'),
            url(r"^(?P<resource_name>%s)/drag%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('attributes_drag_drop'),
                name='api_attributes_drag'),
            url(r"^(?P<resource_name>%s)/specialty%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('fetch_specialty_attributes'),
                name='api_fetch_specialty_attributes'),
            url(r"^(?P<resource_name>%s)/gender%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('gender_attribute'),
                name='api_gender_attribute'),
            url(r"^(?P<resource_name>%s)/compliance/drug-codes%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('get_drug_codes_table'),
                name='api_get_compliance_table'),
            url(r"^(?P<resource_name>%s)/compliance/insights%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('generate_compliance_graph'),
                name='api_generate_compliance_graph'),
            url(r"^(?P<resource_name>%s)/compliance/average-compliance%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('average_compliance_over_time'),
                name='api_average_compliance_over_time'),
            url(r"^(?P<resource_name>%s)/compliance/patient-compliance%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('compliant_patients_over_time'),
                name='api_compliant_patients_over_time'),
            url(r"^(?P<resource_name>%s)/compliance/histogram%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('compliance_histogram_graph'),
                name='api_compliance_histogram_graph'),
            url(r"^(?P<resource_name>%s)/persistence/drug-codes%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('get_drug_codes_table'),
                name='api_get_compliance_table'),
            url(r"^(?P<resource_name>%s)/persistence/insights%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('generate_persistence_graph'),
                name='api_generate_persistence_graph'),
        ]

    def fetch_attributes(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        response = mod.fetch_constant_attributes()
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def attributes_mapping(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post', 'delete'])
        self.is_authenticated(request)
        if request.method == "POST":
            data = json.loads(request.body)
            response = mod.attributes_mapping(data)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)
        if request.method == 'DELETE':
            data = json.loads(request.body)
            response = mod.delete_attribute(data)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

    def attributes_drag_drop(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.attributes_drag_drop(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def fetch_specialty_attributes(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.fetch_specialty_attributes(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def gender_attribute(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.gender_attribute(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def get_drug_codes_table(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.get_drug_codes_table(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def generate_compliance_graph(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.generate_compliance_graph(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def average_compliance_over_time(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.average_compliance_over_time(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def compliant_patients_over_time(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.compliant_patients_over_time(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def compliance_histogram_graph(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.compliance_histogram_graph(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)

    def generate_persistence_graph(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        self.is_authenticated(request)
        data = json.loads(request.body)
        response = mod.generate_persistence_graph(data)
        if response.get('success'):
            return self.create_response(request, response)
        return self.error_response(request, response, HttpBadRequest)


class AnalysisResource(Resource):
    class Meta:
        resource_name = 'analysis'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('delete_analysis'),
                name='api_delete_analysis'),
            url(r"^(?P<resource_name>%s)/data-download%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('output_data_download'),
                name='api_delete_analysis'),
            url(r"^(?P<resource_name>%s)/code-download/%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('generated_code_download'),
                name='api_delete_analysis'),
        ]

    def delete_analysis(self, request, *args, **kwargs):
        self.method_check(request, allowed=['delete'])
        self.is_authenticated(request)
        if request.method == 'DELETE':
            project_id = request.GET.get('project_id')
            analysis_id = request.GET.get('analysis_id')
            response = mod.delete_analysis(project_id, analysis_id)
            if response.get('success'):
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)

    def output_data_download(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        project_id = request.GET.get('project_id')
        analysis_id = request.GET.get('analysis_id')
        attribute_id = request.GET.get('attribute_id')
        session_id = request.GET.get('session_id')
        code_type = request.GET.get('code_type')
        lib_type = request.GET.get('lib_type')
        name = mod.get_project_name(project_id)
        file_name = '{}_{}_{}_{}.csv'.format(name, project_id, analysis_id, code_type)
        response = mod.output_data_download(project_id, analysis_id, attribute_id, session_id, code_type, lib_type)
        if response.get('success'):
            df = response.get('results')
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = 'attachment; filename={}'.format(file_name)
            df.to_csv(path_or_buf=response, index=False)
            return response
        return self.error_response(request, response, HttpBadRequest)

    def generated_code_download(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        data = json.loads(request.body)
        response = mod.generated_code_download(data)
        if response.get('success'):
            file_pointer = open(response['results'], "r")
            file_response = HttpResponse(file_pointer, content_type='text/plain')
            file_response['Content-Disposition'] = 'attachment; filename={}'.format(response.get('file_name'))
            os.remove(response.get('results'))
            return file_response
        return self.error_response(request, response, HttpBadRequest)
