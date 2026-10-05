"""Unit tests."""

import importlib
from contextlib import contextmanager
from typing import Any, Dict
from unittest import mock

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.frame import Method

from airflow.provider.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
from airflow.provider.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor

try:
    from airflow.sdk.bases.sensor import BaseSensorOperator  # Airflow 3.x
except ImportError:
    BaseSensorOperator = importlib.import_module(  # Airflow 2.x
        "airflow.sensors.base"
    ).BaseSensorOperator


class TestRabbitMQSensor:
    """Tests for RabbitMQSensor"""

    conn_id = "rabbitmq_default"
    queue = "test_queue"
    task_id = "test_task_id"

    async def test_init(self):
        """Test sensor initialization"""
        # Test with default conn_id
        sensor1 = RabbitMQSensor(
            task_id=self.task_id,
            queue_name=self.queue,
        )

        assert sensor1.conn_id == self.conn_id
        assert sensor1.queue_name == self.queue
        assert sensor1.queue != self.queue  # still the executor queue
        assert sensor1.auto_ack is True
        assert isinstance(sensor1, BaseSensorOperator)

        # Test with conn_id
        sensor2 = RabbitMQSensor(
            task_id=self.task_id,
            conn_id="test_conn",
            queue_name=self.queue,
            auto_ack=False,
        )

        assert sensor2.conn_id == "test_conn"
        assert sensor2.auto_ack is False

    async def test_template_fields(self):
        """Test template fields"""
        assert "queue_name" in RabbitMQSensor.template_fields
        assert "queue" not in RabbitMQSensor.template_fields

    def test_executor_queue_is_separate_from_rabbitmq_queue(self) -> None:
        """queue sets the executor queue, queue_name the RabbitMQ queue"""
        sensor = RabbitMQSensor(
            task_id=self.task_id, queue_name=self.queue, queue="celery_queue"
        )

        assert sensor.queue_name == self.queue
        assert sensor.queue == "celery_queue"

    def test_dag_with_sensor_serializes(self) -> None:
        """Airflow can serialize a DAG using the sensor, as the DAG processor does"""
        serialized_objects = importlib.import_module(
            "airflow.serialization.serialized_objects"
        )
        if hasattr(serialized_objects, "DagSerialization"):  # Airflow 3.x
            dag_class = importlib.import_module("airflow.sdk").DAG
            serializer = serialized_objects.DagSerialization
        else:  # Airflow 2.x
            dag_class = importlib.import_module("airflow.models.dag").DAG
            serializer = serialized_objects.SerializedDAG

        with dag_class(dag_id="sensor_serialization", schedule=None) as dag:
            RabbitMQSensor(task_id=self.task_id, queue_name=self.queue)

        serialized = serializer.to_dict(dag)

        (task,) = serialized["dag"]["tasks"]
        task = task.get("__var", task)
        assert task["queue_name"] == self.queue

    @mock.patch.object(RabbitMQHook, "get_sync_connection_cm")
    @mock.patch.object(RabbitMQHook, "__init__")
    async def test_poke_with_message(self, mock_hook_init, mock_get_sync_connection_cm):
        """Test poke method when a message is found"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_channel = mock.MagicMock(spec=BlockingChannel)
        mock_method_frame = mock.MagicMock(spec=Method)

        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_get.return_value = (mock_method_frame, None, b"test message")

        # Setup context manager
        @contextmanager
        def mock_cm():
            yield mock_connection

        mock_get_sync_connection_cm.return_value = mock_cm()

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            queue_name=self.queue,
            auto_ack=True,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(conn_id=self.conn_id)
        mock_get_sync_connection_cm.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_channel.basic_get.assert_called_once_with(self.queue, auto_ack=True)
        assert result is True

    @mock.patch.object(RabbitMQHook, "get_sync_connection_cm")
    @mock.patch.object(RabbitMQHook, "__init__")
    async def test_poke_without_message(
        self, mock_hook_init, mock_get_sync_connection_cm
    ):
        """Test poke method when no message is found"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_channel = mock.MagicMock(spec=BlockingChannel)

        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_get.return_value = (None, None, None)

        # Setup context manager
        @contextmanager
        def mock_cm():
            yield mock_connection

        mock_get_sync_connection_cm.return_value = mock_cm()

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            queue_name=self.queue,
            auto_ack=False,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(conn_id=self.conn_id)
        mock_get_sync_connection_cm.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_channel.basic_get.assert_called_once_with(self.queue, auto_ack=False)
        assert result is False

    @mock.patch.object(RabbitMQHook, "get_sync_connection_cm")
    @mock.patch.object(RabbitMQHook, "__init__")
    async def test_poke_with_exception(
        self, mock_hook_init, mock_get_sync_connection_cm
    ):
        """Test poke method when an exception occurs"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_get_sync_connection_cm.side_effect = ConnectionError("Test exception")

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            queue_name=self.queue,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(conn_id=self.conn_id)
        mock_get_sync_connection_cm.assert_called_once()
        assert result is False
