from django.contrib import admin

from attributes.models import AgeGroupAttributes
from attributes.models import GenderAttributes
from attributes.models import ProviderSpeciality


class AgeGroupAdmin(admin.ModelAdmin):
    list_display = ['name',
                    'age_group',
                    'lower_limit_mapping',
                    'upper_limit_mapping',
                    ]


class GenderAdmin(admin.ModelAdmin):
    list_display = ['name',
                    'gender',
                    'mappings',
                    ]


class ProviderSpecialityAdmin(admin.ModelAdmin):
    list_display = ['name',
                    'provider',
                    'mappings',
                    ]


admin.site.register(AgeGroupAttributes, AgeGroupAdmin)
admin.site.register(GenderAttributes, GenderAdmin)
admin.site.register(ProviderSpeciality, ProviderSpecialityAdmin)
