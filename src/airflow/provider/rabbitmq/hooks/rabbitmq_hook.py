"""RabbitMQ hook with synchronous (pika) and asynchronous (aio-pika) support."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Generator, Mapping, Optional, Union, cast
from urllib.parse import quote

import aio_pika
import pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

try:
    from airflow.sdk.bases.hook import BaseHook  # Airflow 3.x
except ImportError:
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined, no-redef]


def _quote(value: Optional[str]) -> str:
    """Percent-encode a URI component, including '/'."""
    return quote(value or "", safe="")


# AMQP basic properties that both pika and aio-pika accept.
MESSAGE_PROPERTIES = frozenset(
    {
        "content_type",
        "content_encoding",
        "headers",
        "delivery_mode",
        "priority",
        "correlation_id",
        "reply_to",
        "expiration",
        "message_id",
        "timestamp",
        "type",
        "user_id",
        "app_id",
    }
)


def check_property_names(properties: Optional[Mapping[str, Any]]) -> None:
    """Raise ValueError on keys that aren't AMQP basic properties."""
    unknown = sorted(set(properties or {}) - MESSAGE_PROPERTIES)
    if unknown:
        raise ValueError(
            f"Unknown message properties: {', '.join(unknown)}. "
            f"Supported: {', '.join(sorted(MESSAGE_PROPERTIES))}"
        )


def normalize_properties(properties: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """
    Validate message properties and convert them to their AMQP wire form.

    Values may be strings, since templated fields render to strings.
    ``delivery_mode`` is 1 (transient) or 2 (persistent), ``priority`` 0-255,
    ``expiration`` milliseconds (int, numeric string or timedelta) and
    ``timestamp`` seconds since the epoch or a datetime. None values are dropped.

    :return: The properties as pika takes them: ``expiration`` a string of
        milliseconds and ``timestamp`` an int.
    """
    check_property_names(properties)
    props = {k: v for k, v in (properties or {}).items() if v is not None}

    if "delivery_mode" in props:
        props["delivery_mode"] = int(props["delivery_mode"])
        if props["delivery_mode"] not in (1, 2):
            raise ValueError("delivery_mode must be 1 (transient) or 2 (persistent)")
    if "priority" in props:
        props["priority"] = int(props["priority"])
        if not 0 <= props["priority"] <= 255:
            raise ValueError("priority must be between 0 and 255")
    if "expiration" in props:
        expiration = props["expiration"]
        if isinstance(expiration, timedelta):
            milliseconds = int(expiration.total_seconds() * 1000)
        else:
            milliseconds = int(expiration)
        if milliseconds < 0:
            raise ValueError("expiration must not be negative")
        props["expiration"] = str(milliseconds)
    if "timestamp" in props:
        timestamp = props["timestamp"]
        props["timestamp"] = (
            int(timestamp.timestamp())
            if isinstance(timestamp, datetime)
            else int(timestamp)
        )
    if "headers" in props and not isinstance(props["headers"], Mapping):
        raise ValueError("headers must be a dict")
    return props


def _to_bytes(message: Union[str, bytes]) -> bytes:
    return message if isinstance(message, bytes) else message.encode()


def build_pika_properties(
    properties: Optional[Mapping[str, Any]],
) -> pika.BasicProperties:
    """Build pika.BasicProperties from the ``properties`` argument."""
    return pika.BasicProperties(**normalize_properties(properties))


def build_aio_pika_message(
    message: Union[str, bytes], properties: Optional[Mapping[str, Any]]
) -> aio_pika.Message:
    """Build an aio_pika.Message, which takes expiration in seconds, not milliseconds."""
    props: dict[str, Any] = normalize_properties(properties)
    if "expiration" in props:
        props["expiration"] = timedelta(milliseconds=int(props["expiration"]))
    if "delivery_mode" in props:
        props["delivery_mode"] = aio_pika.DeliveryMode(props["delivery_mode"])
    return aio_pika.Message(body=_to_bytes(message), **props)


class RabbitMQHook(BaseHook):
    """
    Hook for interacting with RabbitMQ. Supports both synchronous and asynchronous messaging.

    This hook inherits from Airflow's BaseHook and provides methods for connecting to RabbitMQ
    and publishing messages both synchronously and asynchronously.

    :param connection_uri: RabbitMQ connection string (e.g., "amqp://user:password@host:port/vhost").
                          If not provided, it will be retrieved from the Airflow connection.
    :param conn_id: The Airflow connection id to use. The default is "rabbitmq_default".
    """

    conn_name_attr = "conn_id"
    default_conn_name = "rabbitmq_default"
    conn_type = "rabbitmq"
    hook_name = "RabbitMQ"

    def __init__(
        self, connection_uri: Optional[str] = None, conn_id: str = default_conn_name
    ) -> None:
        """
        Initialize RabbitMQ connection settings.

        :param connection_uri: RabbitMQ connection string (e.g., "amqp://user:password@host:port/vhost").
                              If not provided, it will be retrieved from the Airflow connection.
        :param conn_id: The Airflow connection id to use. The default is "rabbitmq_default".
        """
        super().__init__()
        self.conn_id = conn_id
        self._connection_uri: Optional[str] = connection_uri

    @property
    def connection_uri(self) -> str | None:
        """
        Get the RabbitMQ connection URI.

        If a connection_uri was provided during initialization, it will be used.
        Otherwise, the connection URI will be retrieved from the Airflow connection.

        :return: The RabbitMQ connection URI.
        """
        if self._connection_uri:
            return self._connection_uri

        conn = self.get_connection(self.conn_id)
        if conn.host and conn.port:
            # Login, password and vhost are percent-encoded so characters like @, /
            # and # stay part of the value. The colon is kept when the password is
            # empty: pika fails on a login with no password part.
            user_pass = ""
            if conn.login or conn.password:
                user_pass = f"{_quote(conn.login)}:{_quote(conn.password)}@"

            vhost = f"/{_quote(conn.schema)}" if conn.schema else ""

            return f"amqp://{user_pass}{conn.host}:{conn.port}{vhost}"
        if conn.extra_dejson.get("connection_uri"):
            return cast(str, conn.extra_dejson.get("connection_uri"))
        raise ValueError(
            f"No valid connection URI found in connection {self.conn_id}. "
            "Either provide host/port/login/password or connection_uri in the connection."
        )

    @contextmanager
    def get_sync_connection_cm(self) -> Generator[BlockingConnection, None, None]:
        """
        Context manager for synchronous RabbitMQ connections.

        :return: A BlockingConnection instance.
        :raises: ConnectionError if the connection cannot be established.
        """
        conn = None
        try:
            self.log.info("Establishing synchronous connection to RabbitMQ")
            params = pika.URLParameters(self.connection_uri)
            conn = pika.BlockingConnection(params)
            yield conn
        except Exception as e:
            self.log.error("Error connecting to RabbitMQ: %s", str(e))
            raise ConnectionError(f"Failed to connect to RabbitMQ: {str(e)}") from e
        finally:
            if conn is not None and conn.is_open:
                self.log.info("Closing synchronous connection to RabbitMQ")
                conn.close()

    def get_sync_connection(self) -> BlockingConnection:
        """
        Establish a synchronous connection to RabbitMQ using pika.

        :return: A BlockingConnection instance.
        :raises: ConnectionError if the connection cannot be established.
        """
        try:
            self.log.info("Establishing synchronous connection to RabbitMQ")
            params = pika.URLParameters(self.connection_uri)
            return pika.BlockingConnection(params)
        except Exception as e:
            self.log.error("Error connecting to RabbitMQ: %s", str(e))
            raise ConnectionError(f"Failed to connect to RabbitMQ: {str(e)}") from e

    async def get_async_connection(self) -> AbstractRobustConnection:
        """
        Establish an asynchronous connection to RabbitMQ using aio-pika.

        :return: An aio_pika AbstractRobustConnection instance.
        :raises: ConnectionError if the connection cannot be established.
        """
        try:
            self.log.info("Establishing asynchronous connection to RabbitMQ")
            return await aio_pika.connect_robust(self.connection_uri)
        except Exception as e:
            self.log.error("Error connecting to RabbitMQ: %s", str(e))
            raise ConnectionError(f"Failed to connect to RabbitMQ: {str(e)}") from e

    def publish_sync(
        self,
        message: Union[str, bytes],
        exchange: str,
        routing_key: str,
        properties: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Publish a message to RabbitMQ synchronously.

        :param message: The message to be sent. A str is UTF-8 encoded.
        :param exchange: The name of the RabbitMQ exchange.
        :param routing_key: The routing key for message delivery.
        :param properties: AMQP basic properties, see ``normalize_properties``.
        :raises: ValueError if a property is unknown or invalid.
        :raises: ConnectionError if the connection cannot be established.
        :raises: Exception if the message cannot be published.
        """
        pika_properties = build_pika_properties(properties)
        with self.get_sync_connection_cm() as conn:
            try:
                self.log.info("Creating channel for publishing message")
                channel: BlockingChannel = conn.channel()

                self.log.info(
                    "Publishing message to exchange '%s' with routing key '%s'",
                    exchange,
                    routing_key,
                )
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=_to_bytes(message),
                    properties=pika_properties,
                )
                self.log.info("Message published successfully")
            except Exception as e:
                self.log.error("Error publishing message: %s", str(e))
                raise

    async def publish_async(
        self,
        message: Union[str, bytes],
        exchange: str,
        routing_key: str,
        properties: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Publish a message to RabbitMQ asynchronously.

        :param message: The message to be sent. A str is UTF-8 encoded.
        :param exchange: The name of the RabbitMQ exchange.
        :param routing_key: The routing key for message delivery.
        :param properties: AMQP basic properties, see ``normalize_properties``.
        :raises: ValueError if a property is unknown or invalid.
        :raises: ConnectionError if the connection cannot be established.
        :raises: Exception if the message cannot be published.
        """
        outgoing = build_aio_pika_message(message, properties)
        connection = None
        try:
            connection = await self.get_async_connection()
            self.log.info("Creating channel for publishing message asynchronously")
            channel: AbstractChannel = await connection.channel()

            # A named exchange is declared passively, so a missing one raises instead
            # of the broker silently dropping the message.
            target: AbstractExchange = (
                await channel.get_exchange(exchange, ensure=True)
                if exchange
                else channel.default_exchange
            )

            self.log.info(
                "Publishing message asynchronously to exchange '%s' with routing key '%s'",
                exchange,
                routing_key,
            )
            await target.publish(outgoing, routing_key=routing_key)
            self.log.info("Message published successfully")
        except Exception as e:
            self.log.error("Error publishing message asynchronously: %s", str(e))
            raise
        finally:
            if connection is not None:
                self.log.info("Closing asynchronous connection to RabbitMQ")
                await connection.close()
