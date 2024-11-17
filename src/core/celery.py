from __future__ import absolute_import
import os
from celery import Celery
from django.conf import settings

# Установка переменной окружения, чтобы указать Django, какую настройку использовать
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'server.settings')

# Создание экземпляра приложения Celery
app = Celery('server')


app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))