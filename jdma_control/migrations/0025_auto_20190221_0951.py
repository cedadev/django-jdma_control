# Generated by Django 2.1.7 on 2019-02-21 09:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('jdma_control', '0024_auto_20181005_1001'),
    ]

    operations = [
        migrations.AlterField(
            model_name='migration',
            name='stage',
            field=models.IntegerField(choices=[(0, 'ON_DISK'), (1, 'PUTTING'), (2, 'ON_STORAGE'), (3, 'FAILED'), (4, 'DELETING'), (5, 'DELETED')], default=3),
        ),
        migrations.AlterField(
            model_name='migrationrequest',
            name='stage',
            field=models.IntegerField(choices=[(0, 'PUT_START'), (2, 'PUT_PENDING'), (3, 'PUT_PACKING'), (4, 'PUTTING'), (5, 'VERIFY_PENDING'), (6, 'VERIFY_GETTING'), (7, 'VERIFYING'), (8, 'PUT_TIDY'), (9, 'PUT_COMPLETED'), (100, 'GET_START'), (101, 'GET_PENDING'), (102, 'GETTING'), (103, 'GET_UNPACKING'), (104, 'GET_RESTORE'), (105, 'GET_TIDY'), (106, 'GET_COMPLETED'), (200, 'DELETE_START'), (201, 'DELETE_PENDING'), (202, 'DELETING'), (203, 'DELETE_TIDY'), (204, 'DELETE_COMPLETED'), (1000, 'FAILED')], db_index=True, default=1000, help_text='Current upload / download stage'),
        ),
        migrations.AlterField(
            model_name='storagequota',
            name='storage',
            field=models.IntegerField(choices=[(0, 'elastictape'), (1, 'objectstore')], db_index=True, default=0),
        ),
    ]
