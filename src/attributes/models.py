from django.db import models
from projects.models import Projects
from jsonfield import JSONField

# Create your models here.


class AgeGroupAttributes(models.Model):
    name = models.CharField(max_length=100, blank=True, null=True)
    age_group = models.CharField(max_length=100, blank=True, null=True)
    lower_limit_mapping = models.IntegerField(blank=True, null=True)
    upper_limit_mapping = models.IntegerField(blank=True, null=True)


class GenderAttributes(models.Model):
    name = models.CharField(max_length=100, blank=True, null=True)
    gender = models.CharField(max_length=100, blank=True, null=True)
    mappings = models.CharField(max_length=100, blank=True, null=True)


class ProviderSpeciality(models.Model):
    name = models.CharField(max_length=100, blank=True, null=True)
    provider = models.CharField(max_length=100, blank=True, null=True)
    mappings = models.CharField(max_length=500, blank=True, null=True)
    taxonomy_code = models.CharField(max_length=100, blank=True, null=True)


class AttributeMappings(models.Model):
    project = models.ForeignKey(Projects, on_delete=models.CASCADE, blank=True, null=True)
    analysis_id = models.CharField(max_length=50, blank=True, null=True)
    attribute_id = models.CharField(max_length=50, blank=True, null=True)
    attribute_type = models.CharField(max_length=50, blank=True, null=True)
    attribute_name = models.CharField(max_length=100, blank=True, null=True)
    session_id = models.CharField(max_length=50, blank=True, null=True)
    lib_type = models.CharField(max_length=50, blank=True, null=True)
    mappings = JSONField(blank=True, null=True)
    dimensions = JSONField(blank=True, null=True)
