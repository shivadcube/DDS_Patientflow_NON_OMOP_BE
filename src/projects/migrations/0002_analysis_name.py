# -*- coding: utf-8 -*-
# Generated by Django 1.10 on 2020-03-04 09:10
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('projects', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='analysis',
            name='name',
            field=models.CharField(blank=True, max_length=300, null=True),
        ),
    ]
