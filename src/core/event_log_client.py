import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, List, Dict

import clickhouse_connect
import structlog
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.utils import timezone

from core.base_model import Model

logger = structlog.get_logger(__name__)

EVENT_LOG_COLUMNS = [
    'event_type',
    'event_date_time',
    'environment',
    'event_context',
    'metadata_version',
    'is_processed',  # Добавляем поле для отслеживания статуса обработки
]


class EventLogClient:
    def __init__(self, client: clickhouse_connect.driver.Client) -> None:
        self._client = client

    @classmethod
    @contextmanager
    def init(cls) -> Generator['EventLogClient']:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            query_retries=2,
            connect_timeout=30,
            send_receive_timeout=10,
        )
        try:
            yield cls(client)
        except Exception as e:
            logger.error('Error while executing ClickHouse query', error=str(e))
        finally:
            client.close()

    def insert(self, data: List[Model]) -> None:
        if not data:
            logger.warning("No data to insert into ClickHouse.")
            return

        batch = self._convert_data(data)
        try:
            self._client.insert(
                data=batch,
                column_names=EVENT_LOG_COLUMNS,
                database=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            )
            logger.info("Successfully inserted events into ClickHouse", count=len(batch))
        except DatabaseError as e:
            logger.error('Unable to insert data to ClickHouse', error=str(e))

    def query(self, query: str) -> Any:
        logger.debug('Executing ClickHouse query', query=query)

        try:
            return self._client.query(query).result_rows
        except DatabaseError as e:
            logger.error('Failed to execute ClickHouse query', error=str(e))
            return

    def _convert_data(self, data: List[Model]) -> List[Dict[str, Any]]:
        return [
            {
                'event_type': self._to_snake_case(event.__class__.__name__),
                'event_date_time': timezone.now(),
                'environment': settings.ENVIRONMENT,
                'event_context': event.model_dump_json(),
                'metadata_version': 1,  # Укажите версию метаданных
                'is_processed': 0,  # Установите по умолчанию как "не обработано"
            }
            for event in data
        ]

    def _to_snake_case(self, event_name: str) -> str:
        result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', event_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', result).lower()

    def process_outbox(self) -> None:
        """Обработка событий из outbox и запись их в основную таблицу."""
        try:
            events = self._client.query(
                "SELECT * FROM outbox WHERE is_processed = 0 LIMIT 100"
            ).result_rows

            if not events:
                logger.info("No new events to process from outbox.")
                return

            # Пакетируем события для вставки
            batch = [
                (
                    event['event_type'],
                    event['event_date_time'],
                    event['environment'],
                    event['event_context'],
                    event['metadata_version'],
                )
                for event in events
            ]

            # Вставляем пакет в основную таблицу
            self._client.insert(
                data=batch,
                column_names=EVENT_LOG_COLUMNS[:-1],  # Исключаем is_processed
                database=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            )

            # Обновляем статус обработанных событий
            ids = [event['id'] for event in events]
            self._client.execute(f"ALTER TABLE outbox UPDATE is_processed = 1 WHERE id IN ({','.join(ids)})")
            logger.info("Processed and moved events from outbox to main table", count=len(events))

        except DatabaseError as e:
            logger.error('Failed to process events from outbox', error=str(e))