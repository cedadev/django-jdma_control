# Generated by Django 2.0.2 on 2018-02-27 16:03

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0011_auto_20180223_1146'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='migration',
            name='failure_reason',
        ),
    ]
