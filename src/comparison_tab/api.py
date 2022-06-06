import json
from django.conf.urls import url
from django.http import HttpResponse

from tastypie.utils.urls import trailing_slash
from tastypie.resources import Resource

import csv


class ComparisonTabResource(Resource):
    class Meta:
        resource_name = 'comparison_tab'

    def prepend_urls(self):
        return [
            url(r"^(?P<resource_name>%s)/download%s$" %
                (self._meta.resource_name, trailing_slash()),
                self.wrap_view('crosstab_download'),
                name='api_crosstab_download'),
        ]

    def crosstab_download(self, request, *args, **kwargs):
        self.method_check(request, allowed=['post'])
        body = json.loads(request.body)
        title = body.get('title', "title")+".csv"
        x_axis = body.get('xAxis', '')
        y_axis = body.get('yAxis', '')
        data = body.get('data', {})
        axis = {}
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename={}'.format(title)
        writer = csv.writer(response)

        if x_axis and not y_axis:
            axis = x_axis
        elif y_axis and not x_axis:
            axis = y_axis
        if axis:
            header = [axis.get('name'), 'values']
            writer.writerow(header)
            for key, val in data.items():
               writer.writerow([key, val])
        else:
            header = [y_axis.get('name', "unnamed attribute"), x_axis.get('name', "unnamed attribute"), 'values']
            writer.writerow(header)
            for x_key, x_value in data.items():
                for key, value in x_value.items():
                    writer.writerow([x_key, key, value])

        return response
