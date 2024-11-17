# Generated by Django 5.1.2 on 2024-11-17 18:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='EventOutbox',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('event_type', models.CharField(max_length=255)),
                ('event_date_time', models.DateTimeField(auto_now_add=True)),
                ('environment', models.CharField(max_length=50)),
                ('event_context', models.JSONField()),
                ('metadata_version', models.PositiveIntegerField()),
                ('is_processed', models.BooleanField(default=False)),
            ],
            options={
                'db_table': 'event_outbox',
                'ordering': ['-event_date_time'],
            },
        ),
    ]