"""Sensor that waits for messages in a RabbitMQ queue."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence

from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from pika.frame import Method

from airflow.provider.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator  # Airflow 3.x
except ImportError:
    from airflow.sensors.base import BaseSensorOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    try:
        from airflow.sdk.bases.sensor import PokeReturnValue  # Airflow 3.x
    except ImportError:
        from airflow.sensors.base import PokeReturnValue  # type: ignore[no-redef]
    try:
        from airflow.sdk.definitions.context import Context  # Airflow 3.x
    except ImportError:
        from airflow.utils.context import Context  # type: ignore[attr-defined,no-redef]


class RabbitMQSensor(BaseSensorOperator):  # pylint: disable=too-many-ancestors
    """
    Airflow Sensor to wait for messages in a RabbitMQ queue.

    This sensor periodically checks a specified RabbitMQ queue and triggers
    downstream tasks once a message is detected.

    :param queue: The name of the RabbitMQ queue to monitor.
    :param conn_id: The Airflow connection id to use. Default is "rabbitmq_default".
    :param auto_ack: Whether to automatically acknowledge the message. Default is True.
    """

    template_fields: Sequence[str] = ("queue",)
    ui_color = "#f0ede4"

    def __init__(
        self,
        queue: str,
        conn_id: str = "rabbitmq_default",
        auto_ack: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the RabbitMQSensor.

        :param queue: The name of the RabbitMQ queue to monitor.
            :param conn_id: The Airflow connection id to use. Default is "rabbitmq_default".
        :param auto_ack: Whether to automatically acknowledge the message. Default is True.
        """
        super().__init__(**kwargs)
        self.conn_id: str = conn_id
        self.queue: str = queue
        self.auto_ack: bool = auto_ack

    def poke(self, _context: Context) -> bool | PokeReturnValue:
        """
        Checks the RabbitMQ queue for new messages.

        :param _context: Airflow's execution context dictionary (unused).
        :return: True if a message is found; otherwise, False.
        """
        hook = RabbitMQHook(conn_id=self.conn_id)
        try:
            with hook.get_sync_connection_cm() as conn:
                channel: BlockingChannel = conn.channel()

                # Attempt to retrieve a message without consuming it
                method_frame: Optional[Method]
                method_frame, _, body = channel.basic_get(
                    self.queue, auto_ack=self.auto_ack
                )

                if method_frame:
                    self.log.info("Received message: %s", body)
                    return True

        except (ConnectionError, AMQPError) as e:
            self.log.error("Error during RabbitMQ poke: %s", e)

        return False
