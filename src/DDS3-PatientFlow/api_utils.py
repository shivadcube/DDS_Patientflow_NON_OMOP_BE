"""Utils module for API."""

import json
import logging

from django.conf import settings
from django.contrib.auth.models import User
from django.http import HttpResponse
from django.utils import timezone
from tastypie.exceptions import ImmediateHttpResponse
from tastypie.serializers import Serializer
from tastypie.throttle import CacheThrottle

from DDS3_ADMIN import api_constants
from DDS3_ADMIN import utils


logger = logging.getLogger(__name__)


class Required(object):
    """Annotation for specifying required fields for TastyPie API."""

    def __init__(self, params, get_json_body=True):
        self._params = params
        self._get_json_body = get_json_body

    def _get_req(self, request):
        if request.method in ['POST', 'PUT']:
            try:
                return json.loads(request.body)
            except ValueError as e:
                raise_json_response(status=407)
        else:
            return request.GET

    def __call__(self, func):
        def inner(resource, request, *args, **kwargs):
            req = self._get_req(request)
            missing_params = [fld for fld in self._params if fld not in req]
            if missing_params:
                return resource.error_response(request, {
                        'success': False,
                        'fields': missing_params,
                        'reason': 'requiredFieldsNotPresent'
                    })
            additional_args = []
            for param in self._params:
                additional_args.append(req[param])
            if request.method in ['POST', 'PUT'] and self._get_json_body:
                additional_args.append(req)
            args = tuple(additional_args) + args
            return func(resource, request, *args, **kwargs)
        return inner


class JsonResponse(HttpResponse):
    def __init__(self, status, body=None):
        super(JsonResponse, self).__init__(status=status, content_type='application/json')
        if not body:
            body = {}
        self.content = json.dumps(body)


def raise_json_response(status, body=None):
    raise ImmediateHttpResponse(response=JsonResponse(status, body))


def get_accessee_user(kwargs, accessor):
    if 'user_name' not in kwargs or kwargs['user_name'] == 'me':
        # Accessor and accessee are the same user.
        return accessor
    try:
        username = urllib2.unquote(kwargs['user_name'])
        return User.objects.get_by_natural_key(username)
    except User.DoesNotExist:
        raise_json_response(404, body={
            'errorCode': 'userNotFound',
            'value': '%s' % kwargs['user_name']
        })


def get_accessor_user(username, accessor, is_admin=False):
    if not is_admin:
        return accessor
    if not username or username == 'me':
        # Accessor and accessee are the same user.
        return accessor
    try:
        return User.objects.get_by_natural_key(username)
    except User.DoesNotExist:
        raise_json_response(404, body={
            'errorCode': 'userNotFound',
            'value': '%s' % username
        })


class CustomCacheThrottle(CacheThrottle):
    def should_be_throttled(self, identifier, **kwargs):
        """ An overridden implementation of the same method to ignore throttling for DummyCache
        backend. """
        cache = getattr(settings, 'CACHES', {})
        cache_default = cache.get('default')

        # bug in tastypie throttling, check to pass throttling while running tests
        if cache_default and cache_default['BACKEND'].endswith('DummyCache'):
            return False
        try:
            throttle = super(CustomCacheThrottle, self).should_be_throttled(identifier, **kwargs)
            if throttle:
                # Throttle limit exceeded.
                warning_message = '{} has reached their throttle limit'.format(identifier)
                logger.error(warning_message)
            return throttle
        except TypeError as e:
            logger.exception(str(e))


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def get_sync_token_datetime(sync_token):
    """Return sync_token convered to datetime object from timestamp
    """
    try:
        sync_token = utils.convert_timestamp_to_datetime(
                float(sync_token))
        return min(sync_token, timezone.now())
    except (ValueError, TypeError):
        return utils.convert_timestamp_to_datetime(0)


def get_error_response(error_code, error_message):
    return {
        api_constants.API_ERROR_CODE: error_code,
        api_constants.API_ERROR_MESSAGE: error_message
    }


def get_object_or_raise_error(id, model, error_resp):
    """This function takes an id, 'Model'(Django Model) and an errorCode and
    errorMessage, and returns an object with given id.
    If the object doesn't exist then it raises an error with the given
    error code and message
    """
    try:
        obj = model.objects.get(id=id)
    except model.DoesNotExist:
        raise ImmediateHttpResponse(error_resp)
    return obj


def get_old_style_error_response(error_code, error_message):
    """Returns old style error response with new response.
    Used for migration from old to new type.
    """
    new_response = get_error_response(error_code, error_message)
    old_response = {
        'success': False,
        'reason': error_message,
    }
    old_response.update(new_response)
    return old_response


def append_trailing_slash(uri):
    if uri.endswith('/'):
        return uri
    return uri + '/'


def is_success_response_code(response):
    return str(response.status_code).startswith('2')


def get_pagination_param(request):
    limit = int(request.GET.get('limit', api_constants.DEFAULT_LIMIT))
    offset = int(request.GET.get('offset', api_constants.DEFAULT_OFFSET))
    return limit, offset


class DateSerializer(Serializer):
    def format_datetime(self, date):
        # If naive or rfc-2822, default behavior...
        date = date.replace(microsecond=0)
        if timezone.is_naive(date) or self.datetime_formatting == 'rfc-2822':
            return super(DateSerializer, self).format_datetime(date)

        return date.isoformat()
