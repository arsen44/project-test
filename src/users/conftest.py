import pytest
from django.test import TestCase
from core.event_log_client import EventLogClient
from .models import EventOutbox

class TestEventLog(TestCase):
    def test_event_outbox_processing(self):
        # Создаем тестовые события
        events = [
            EventOutbox(
                event_type="TestEvent",
                environment="test",
                event_context={"key": "value"},
                metadata_version=1,
            )
            for _ in range(10)
        ]
        EventOutbox.objects.bulk_create(events)

        # Обрабатываем outbox
        with EventLogClient.init() as client:
            client.process_outbox()

        # Проверяем, что события были обработаны и записаны в ClickHouse
        processed_events = EventOutbox.objects.filter(is_processed=True)
        self.assertEqual(processed_events.count(), 10)