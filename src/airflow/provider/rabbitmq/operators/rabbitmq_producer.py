"""Operator for publishing messages to RabbitMQ."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

from airflow.models import BaseOperator

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context  # Airflow 3.x
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined,no-redef]

from airflow.provider.rabbitmq.hooks.rabbitmq_hook import (
    RabbitMQHook,
    check_property_names,
)


class RabbitMQProducerOperator(BaseOperator):
    """
    Airflow Operator for publishing messages to RabbitMQ.

    Supports both synchronous (blocking) and asynchronous message publishing.

    :param conn_id: The Airflow connection id to use. Default is "rabbitmq_default".
    :param message: The message to be sent to RabbitMQ, as str (UTF-8 encoded) or bytes.
    :param exchange: The RabbitMQ exchange name.
    :param routing_key: The routing key used for routing the message.
    :param use_async: Flag to determine whether to use async messaging. Defaults to False.
    :param properties: AMQP basic properties (content_type, headers, delivery_mode,
        priority, correlation_id, expiration in milliseconds, ...). Templated.
        Unknown keys raise ValueError.
    """

    template_fields: Sequence[str] = (
        "message",
        "exchange",
        "routing_key",
        "properties",
    )
    ui_color = "#f0e4d5"

    def __init__(
        self,
        message: Union[str, bytes],
        exchange: str,
        routing_key: str,
        conn_id: str = "rabbitmq_default",
        use_async: bool = False,
        properties: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the RabbitMQProducerOperator.

        :param message: The message to be sent to RabbitMQ, as str or bytes.
        :param exchange: The RabbitMQ exchange name.
        :param routing_key: The routing key used for routing the message.
        :param conn_id: The Airflow connection id to use. Default is "rabbitmq_default".
        :param use_async: Flag to determine whether to use async messaging. Defaults to False.
        :param properties: AMQP basic properties for the message.
        """
        super().__init__(**kwargs)
        # Values can be templates, so only the names are checked here; the hook
        # validates the rendered values.
        check_property_names(properties)
        self.conn_id: str = conn_id
        self.message: Union[str, bytes] = message
        self.exchange: str = exchange
        self.routing_key: str = routing_key
        self.use_async: bool = use_async
        self.properties: Optional[dict[str, Any]] = properties

    def execute(self, context: Context) -> Any:
        """
        Executes the operator by publishing a message to RabbitMQ.

        Uses either synchronous (`pika`) or asynchronous (`aio-pika`) based on the `use_async` flag.

        :param context: Airflow's execution context dictionary.
        """
        hook = RabbitMQHook(conn_id=self.conn_id)

        try:
            if self.use_async:
                self.log.info("Publishing message asynchronously to RabbitMQ")
                asyncio.run(
                    hook.publish_async(
                        self.message,
                        self.exchange,
                        self.routing_key,
                        properties=self.properties,
                    )
                )
            else:
                self.log.info("Publishing message synchronously to RabbitMQ")
                hook.publish_sync(
                    self.message,
                    self.exchange,
                    self.routing_key,
                    properties=self.properties,
                )
            self.log.info("Message published successfully")
        except Exception as e:
            self.log.error("Failed to publish message to RabbitMQ: %s", str(e))
            raise
