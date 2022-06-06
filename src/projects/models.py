from jsonfield import JSONField
from django.db import models
# Create your models here.


class Projects(models.Model):

    name = models.CharField(max_length=100, blank=True, null=True)
    tags = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=100, blank=True, null=True)
    # storing metadata of project
    metadata = JSONField(blank=True, null=True)
    session_id = models.CharField(max_length=100, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    modified_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)


class Analysis(models.Model):
    name = models.CharField(max_length=300, blank=True, null=True)
    project = models.ForeignKey(Projects, on_delete=models.CASCADE,
                                blank=True, null=True)
    analysis_data = JSONField(blank=True, null=True)
    # storing uploaded csv file s3 path
    s3_path = models.CharField(max_length=300, blank=True, null=True)
    # id of lot app attributes id
    lot_attributes_id = models.CharField(max_length=10, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    modified_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)


class BackgroundJobs(models.Model):

    project = models.ForeignKey(Projects, on_delete=models.CASCADE, blank=True, null=True)
    analysis = models.CharField(max_length=200, blank=True, null=True)
    cluster_id = models.CharField(max_length=200, blank=True, null=True)
    cluster_status = models.CharField(max_length=200, blank=True, null=True)
    zeppelin_host = models.CharField(max_length=200, blank=True, null=True)
    notebook_id = models.CharField(max_length=200, blank=True, null=True)
    paragraph_id = models.CharField(max_length=200, blank=True, null=True)
    status = models.CharField(max_length=200, blank=True, null=True, default='READY')
    attribute_type = models.CharField(max_length=50, blank=True, null=True)
    error_message = models.CharField(max_length=500, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    modified_at = models.DateTimeField(auto_now=True, blank=True, null=True)


class ClusterDetails(models.Model):

    cluster_id = models.CharField(max_length=200, blank=True, null=True)
    cluster_name = models.CharField(max_length=200, blank=True, null=True)
    cluster_region = models.CharField(max_length=200, blank=True, null=True)
    cluster_file_path = models.CharField(max_length=200, blank=True, null=True)
    cluster_master_ip = models.CharField(max_length=200, blank=True, null=True)
    cluster_status = models.CharField(max_length=200, blank=True, null=True)
    cluster_map_ip_port = models.CharField(max_length=20, blank=True, null=True)
