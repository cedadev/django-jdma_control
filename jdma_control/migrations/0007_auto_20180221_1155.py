# Generated by Django 2.0.2 on 2018-02-21 11:55

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0006_auto_20180221_1104'),
    ]

    operations = [
        migrations.AlterField(
            model_name='migration',
            name='storage',
            field=models.ForeignKey(help_text='External storage location of the migration, e.g. elastictape or objectstore', on_delete=django.db.models.deletion.CASCADE, to='jdma_control.StorageQuota'),
        ),
        migrations.AlterField(
            model_name='migrationrequest',
            name='storage',
            field=models.ForeignKey(help_text='External storage location of the migration, e.g. elastictape or objectstore', on_delete=django.db.models.deletion.CASCADE, to='jdma_control.StorageQuota'),
        ),
    ]
