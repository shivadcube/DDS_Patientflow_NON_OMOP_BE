import dateutil
import functools
import jwt
import os
import pytz
import re
import time
from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from sys import getsizeof

from django.conf import settings
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.db.models.query_utils import Q
from django.http import HttpResponse
from django.http import HttpResponseForbidden
from django.middleware import csrf
from django.template import Context
from django.template import Template
from django.utils import timezone
from django.utils.timezone import now


class IPCheckMiddleware(object):
    """This class will check for inmy allowed intern ip(s) or return 403"""

    def process_request(self, request):
        if request.GET.get('username') and request.GET.get('api_key'):
            # if api ky login, do not do anything
            return

        remote_addr = request.META.get('HTTP_X_REAL_IP') or request.META.get(
            'REMOTE_ADDR')
        if not remote_addr in settings.INTERNAL_IPS:
            return HttpResponse(
                '&#128561; !! you can\'t touch me &#128514; &#128514; '
                '&#128514;  !', status=403)


class TimezoneMiddleware(object):
    """Activates django timezone for the user based on user-profile settings."""

    def process_request(self, request):
        if not request.user.is_authenticated():
            return
        try:
            tz = request.user.profile.timezone
            if not tz:
                return
            timezone.activate(tz)
        except ValueError:
            pass

        except Exception:
            pass


class RemoteUserMiddleware(object):

    def process_response(self, request, response):
        if not hasattr(request, 'user'):
            return response
        if request.user.is_authenticated():
            try:
                response['X-Remote-User-Name'] = request.user.username
                response['X-Remote-User-Id'] = request.user.id
            except TypeError as e:
                pass
        return response


class TransformRequestBody(object):
    """This class is updating request body by removing utf-8 encoding"""

    def process_request(self, request):
        if request.method.upper() in ['POST']:
            if not request.FILES.get("File"):
                data = getattr(request, '_body', request.body)
                request._body = data.decode('utf-8')
            return


class JWTAuthMiddleware(object):

    def _get_token(self, request=None):
        try:
            if request.META.get('HTTP_AUTHORIZATION'):
                return request.META.get('HTTP_AUTHORIZATION')[6:]
        except Exception as e:
            return e

    def process_request(self, request):
        token = self._get_token(request)
        if token is not None:
            try:
                payload = jwt.decode(token, 'secret', algorithms=['HS256'])
                print(User.objects.all())
                request.user = User.objects.get(
                    username=payload.get('username'),
                    pk=payload.get('id'),
                    is_active=True
                )
                return

            except jwt.ExpiredSignatureError:
                return HttpResponseForbidden("Token expired. Get new one")

            except jwt.InvalidTokenError:
                return HttpResponseForbidden("Invalid Token")

            except User.DoesNotExist:
                return HttpResponseForbidden("Invalid User")
        return
    

class CsrfCookieMiddleware(csrf.CsrfViewMiddleware):
    """ Middleware to ensure that the csrf cookie is set for all GET requests.

    Subclasses django's CsrfViewMiddleware.
    """

    def process_view(self, request, callback, callback_args, callback_kwargs):
        """
        Ensures that the csrf cookie is set for all GET requests, by
        calling django.middleware.csrf.get_token, which sets
        request.META["CSRF_COOKIE_USED"] to True.

        This is exactly what django's 'ensure_csrf_cookie' decorator does
        https://github.com/django/django/blob/1.5.1/django/
        views/decorators/csrf.py#L32
        """

        retval = super(CsrfCookieMiddleware, self).process_view(
            request, callback, callback_args, callback_kwargs)

        # Force process_response to set the cookie on all GET requests
        if request.method == "GET":
            csrf.get_token(request)

        return retval
