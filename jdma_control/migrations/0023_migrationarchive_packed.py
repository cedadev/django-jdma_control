# Generated by Django 2.0.6 on 2018-06-25 14:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0022_auto_20180419_1413'),
    ]

    operations = [
        migrations.AddField(
            model_name='migrationarchive',
            name='packed',
            field=models.BooleanField(default=False, help_text='Is the archive packed (tarred)?'),
        ),
    ]
