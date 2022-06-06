from django.contrib.auth.backends import ModelBackend
from django.contrib.auth.models import User

from tastypie.models import ApiKey


class ApiKeyAuthBackend(ModelBackend):
    """Log in to Django with username and apikey"""
    def authenticate(self, username=None, api_key=None):
        try:
            user = User.objects.get(username=username)
            return ApiKey.objects.get(key=api_key, user=user).user
        except (User.DoesNotExist, ApiKey.DoesNotExist):
            return None
