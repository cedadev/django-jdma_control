# -*- coding: utf-8 -*-
# Generated by Django 1.11.5 on 2017-10-12 13:42
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0007_auto_20171012_1341'),
    ]

    operations = [
        migrations.AlterField(
            model_name='migrationrequest',
            name='stage',
            field=models.IntegerField(choices=[(0, 'ON_TAPE'), (1, 'GET_PENDING'), (2, 'GETTING'), (3, 'ON_DISK'), (4, 'FAILED')], default=0),
        ),
    ]
