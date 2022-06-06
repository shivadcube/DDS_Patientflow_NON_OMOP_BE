from django.contrib import admin

from projects.models import Analysis
from projects.models import Projects


class ProjectsAdmin(admin.ModelAdmin):
    list_display = ['name',
                    'tags',
                    'created_by',
                    'metadata',
                    'created_at',
                    'modified_at']


class ProjectsAnalysisAdmin(admin.ModelAdmin):
    list_display = ['project',
                    'analysis_data',
                    's3_path',
                    'lot_attributes_id',
                    'created_at',
                    'modified_at']


admin.site.register(Analysis, ProjectsAnalysisAdmin)
admin.site.register(Projects, ProjectsAdmin)
