# Generated by Django 2.1.7 on 2019-02-21 13:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0026_auto_20190221_1029'),
    ]

    operations = [
        migrations.AddField(
            model_name='migrationfile',
            name='ftype',
            field=models.CharField(default='FILE', help_text='Type of the file', max_length=4),
        ),
    ]