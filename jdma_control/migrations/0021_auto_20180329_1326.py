# Generated by Django 2.0.2 on 2018-03-29 13:26

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0020_auto_20180322_1546'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='migration',
            name='filelist',
        ),
        migrations.AddField(
            model_name='migrationrequest',
            name='filelist',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(blank=True, max_length=1024, unique=True), blank=True, help_text='List of files for uploading or downloading', null=True, size=None),
        ),
    ]
