# -*- coding: utf-8 -*-
# Generated by Django 1.10 on 2020-02-07 10:51
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='AgeGroupAttributes',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=100, null=True)),
                ('age_group', models.CharField(blank=True, max_length=100, null=True)),
                ('lower_limit_mapping', models.CharField(blank=True, max_length=100, null=True)),
                ('upper_limit_mapping', models.CharField(blank=True, max_length=100, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='GenderAttributes',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=100, null=True)),
                ('gender', models.CharField(blank=True, max_length=100, null=True)),
                ('mappings', models.CharField(blank=True, max_length=100, null=True)),
            ],
        ),
    ]
