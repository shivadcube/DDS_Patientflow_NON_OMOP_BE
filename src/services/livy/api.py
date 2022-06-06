import json

from django.conf import settings
from django.conf.urls import url

from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.resources import Resource
from tastypie.utils.urls import trailing_slash

from services.livy import mod


class LivyResource(Resource):

	class Meta:
		resource_name = 'livy'

	def prepend_urls(self):
		return [
			url(r"^(?P<resource_name>%s)/start%s$" % (
				self._meta.resource_name, trailing_slash()),
				self.wrap_view('start_livy'), name="api_start_livy"),
			url(r"^(?P<resource_name>%s)/status/(?P<session_id>[a-zA-Z0-9_]*)%s$" % (
				self._meta.resource_name, trailing_slash()),
				self.wrap_view('livy_status'), name="api_livy_status"),
			url(r"^(?P<resource_name>%s)/stop/(?P<session_id>[a-zA-Z0-9_]*)%s$" % (
				self._meta.resource_name, trailing_slash()),
				self.wrap_view('stop_livy'), name="api_stop_livy"),
		]

	def start_livy(self, request, *args, **kwargs):
		self.method_check(request, allowed=['get'])
		self.is_authenticated(request)
		response = mod.start_session()
		if response.get("success"):
			return self.create_response(request, response)
		return self.error_response(request, response, HttpBadRequest)

	def stop_livy(self, request, session_id, *args, **kwargs):
		self.method_check(request, allowed=['get'])
		self.is_authenticated(request)
		response = mod.stop_session(session_id)
		if response.get("success"):
			return self.create_response(request, response)
		return self.error_response(request, response, HttpBadRequest)

	def livy_status(self, request, session_id, *args, **kwargs):
		self.method_check(request, allowed=['get'])
		self.is_authenticated(request)
		prj_id = None
		if request.GET.get('project_id'):
			prj_id = request.GET.get('project_id')
		response = mod.session_status(session_id, prj_id)
		if response.get("success"):
			return self.create_response(request, response)
		return self.error_response(request, response, HttpBadRequest)
