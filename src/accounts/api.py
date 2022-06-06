#!/usr/bin/python
import json
import jwt

from django.conf.urls import url
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.contrib.auth import authenticate
from tastypie.authentication import Authentication
from tastypie.authorization import DjangoAuthorization

from tastypie.exceptions import ImmediateHttpResponse
from tastypie.utils.urls import trailing_slash
from tastypie.http import HttpUnauthorized
from tastypie.http import HttpApplicationError
from tastypie.http import HttpBadRequest

from tastypie.http import HttpForbidden
from tastypie.http import HttpNotFound
from tastypie.resources import Resource

from accounts import mod
from django.contrib.auth.models import User

import requests
import datetime

import environ

env = environ.Env()
environ.Env.read_env()


class AccountResource(Resource):
    class Meta:
        resource_name = 'accounts'
        authorization = DjangoAuthorization()

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/login%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('login'), name="api_login"),
            url(r"^(?P<resource_name>%s)/list%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('list'), name="api_list"),
            url(r"^(?P<resource_name>%s)/logout%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('logout'), name="api_logout"),
            url(r"^(?P<resource_name>%s)/signup%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('signup'), name="api_signup"),

        ]

    def login(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        try:
            url = env('AUTH_URL')
            res = requests.post(url=url, data=request.body)
            response = json.loads(res.text)
            if response.get('success', ''):
                user_id = response.get('id', 0)
                email = response.get('email', '')
                username = response.get('username', '')
                mod.insert_user_id(user_id, email, username)
                request.session['user_id'] = user_id
                request.session['token'] = response.get('token', '')
                request.session['session_time'] = str(datetime.datetime.now())
                return self.create_response(request, response)
            return self.error_response(request, response, HttpBadRequest)
        except Exception as e:
            return self.error_response(request, {'error': str(e)}, HttpBadRequest)

    def list(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        x = User.objects.all()
        return self.create_response(request, x)

    def logout(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        response = True
        return self.create_response(request, response)

    def signup(self, request, *args, **kwargs):
        self.method_check(request, allowed=['get'])
        return self.create_response(request, {'success': True})
