"""Integration tests using a RabbitMQ container."""

import json
import os
from typing import Any, Dict
from unittest import mock

import aio_pika
import pika
import pytest

try:
    import docker
    from docker.errors import DockerException
    from testcontainers.rabbitmq import RabbitMqContainer

    # Try to ping docker to see if it's actually running
    docker.from_env().ping()
    DOCKER_AVAILABLE = True
except (ImportError, DockerException, OSError):
    DOCKER_AVAILABLE = False

from airflow.provider.rabbitmq.operators.rabbitmq_producer import (
    RabbitMQProducerOperator,
)
from airflow.provider.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor


@pytest.mark.skipif(not DOCKER_AVAILABLE, reason="Docker is not available")
class TestRabbitMQIntegration:
    """Integration tests for RabbitMQ provider components"""

    queue: str = "test_queue"
    routing_key: str = "test_queue"
    exchange: str = ""
    message: str = "test integration message"
    task_id: str = "test_task_id"
    conn_id: str = "rabbitmq_integration"
    connection_uri: str = None
    named_exchange: str = "test_exchange"
    named_exchange_queue: str = "test_exchange_queue"
    named_exchange_routing_key: str = "test_exchange_key"

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def rabbitmq_container(cls):
        """Start a RabbitMQ container for the test class"""
        with RabbitMqContainer("rabbitmq:4") as container:
            # Allow RabbitMQ to initialize
            params = container.get_connection_params()
            creds = f"{params.credentials.username}:{params.credentials.password}"
            cls.connection_uri = (
                f"amqp://{creds}@{params.host}:{params.port}{params.virtual_host}"
            )

            # Manually configure queue using pika
            params = pika.URLParameters(cls.connection_uri)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=cls.queue, durable=True)
            channel.exchange_declare(
                exchange=cls.named_exchange, exchange_type="direct"
            )
            channel.queue_declare(queue=cls.named_exchange_queue, durable=True)
            channel.queue_bind(
                queue=cls.named_exchange_queue,
                exchange=cls.named_exchange,
                routing_key=cls.named_exchange_routing_key,
            )
            connection.close()  # no need for this connection anymore

            # Expose the broker to the operator and sensor as an Airflow connection
            airflow_conn = json.dumps(
                {
                    "conn_type": "rabbitmq",
                    "extra": {"connection_uri": cls.connection_uri},
                }
            )
            with mock.patch.dict(
                os.environ, {f"AIRFLOW_CONN_{cls.conn_id.upper()}": airflow_conn}
            ):
                yield  # continue with tests

    def test_operator_sensor_integration(self):
        """Test integration between RabbitMQProducerOperator and RabbitMQSensor"""
        # Run the RabbitMQProducerOperator
        operator = RabbitMQProducerOperator(
            task_id=TestRabbitMQIntegration.task_id,
            conn_id=TestRabbitMQIntegration.conn_id,
            message=TestRabbitMQIntegration.message,
            exchange=TestRabbitMQIntegration.exchange,
            routing_key=TestRabbitMQIntegration.routing_key,
            use_async=False,
        )

        context: Dict[str, Any] = {}
        operator.execute(context)

        # Run the RabbitMQSensor to verify the message
        sensor = RabbitMQSensor(
            task_id=TestRabbitMQIntegration.task_id,
            conn_id=TestRabbitMQIntegration.conn_id,
            queue_name=TestRabbitMQIntegration.queue,
            timeout=10,  # seconds
            poke_interval=1,
            mode="poke",
        )

        result = sensor.poke(context)
        assert result is True

    def _get_message(self, queue: str) -> bytes | None:
        """Take one message off a queue, or None if it's empty."""
        connection = pika.BlockingConnection(pika.URLParameters(self.connection_uri))
        try:
            method_frame, _, body = connection.channel().basic_get(queue, auto_ack=True)
            return body if method_frame else None
        finally:
            connection.close()

    def _publish_async(self, message: str, exchange: str, routing_key: str) -> None:
        RabbitMQProducerOperator(
            task_id=self.task_id,
            conn_id=self.conn_id,
            message=message,
            exchange=exchange,
            routing_key=routing_key,
            use_async=True,
        ).execute({})

    def test_async_publish_to_named_exchange(self) -> None:
        """use_async=True routes through the named exchange, not the default one"""
        self._publish_async(
            "via named exchange", self.named_exchange, self.named_exchange_routing_key
        )

        assert self._get_message(self.named_exchange_queue) == b"via named exchange"

    def test_async_publish_to_default_exchange(self) -> None:
        """use_async=True with an empty exchange publishes straight to the queue"""
        self._publish_async("via default exchange", "", self.queue)

        assert self._get_message(self.queue) == b"via default exchange"

    def test_async_publish_to_missing_exchange_fails(self) -> None:
        """use_async=True fails the task when the exchange doesn't exist"""
        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            self._publish_async("lost", "missing_exchange", self.routing_key)
