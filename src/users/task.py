from celery import shared_task
from django.db import transaction
from core.event_log import EventLogClient  # Импорт класса, который управляет записью в ClickHouse
from .models import EventOutbox
import structlog

logger = structlog.get_logger()

@shared_task
def process_outbox_events():
    with transaction.atomic():
        # Извлечение событий, которые еще не были обработаны
        events = list(EventOutbox.objects.filter(is_processed=False)[:100])  # Размер пачки
        if not events:
            return

        try:
            # Используем EventLogClient для записи событий в ClickHouse
            with EventLogClient.init() as client:
                client.insert(events)  # Вставляем события в ClickHouse
            
            # Обновляем статус событий, помечая их как обработанные
            EventOutbox.objects.filter(id__in=[e.id for e in events]).update(is_processed=True)
        
        except Exception as e:
            # Логируем ошибку в случае неудачи
            logger.error("Failed to process outbox events", error=str(e))
            raise

        