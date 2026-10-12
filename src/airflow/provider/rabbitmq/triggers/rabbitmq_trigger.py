"""Trigger that asynchronously monitors a RabbitMQ queue for new messages."""

import asyncio
from functools import partial
from typing import Any, AsyncIterator, Dict, Optional, Sequence

from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustQueue,
)
from airflow.triggers.base import TriggerEvent
from asgiref.sync import sync_to_async

from airflow.provider.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook

try:
    from airflow.providers.common.compat.module_loading import import_string
except ImportError:
    from airflow.utils.module_loading import import_string
try:
    from airflow.triggers.base import BaseEventTrigger
except ImportError:
    from airflow.triggers.base import BaseTrigger as BaseEventTrigger  # type: ignore


class RabbitMQTrigger(BaseEventTrigger):

    def __init__(
        self,
        queue_name: str,
        conn_id: str = "rabbitmq_default",
        connection_uri: Optional[str] = None,
        exchange_type: str = "queue",  # queue, fanout, direct, topic
        exchange_name: Optional[str] = None,
        routing_key: Optional[str] = None,
        requeue_on_error: bool = True,
        poll_interval: int = 10,
        apply_function: Optional[str] = None,
        apply_function_args: Optional[Sequence[Any]] = None,
        apply_function_kwargs: Optional[dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Airflow Trigger to asynchronously wait for messages in a RabbitMQ queue.

        This trigger runs inside the Airflow Triggerer process, continuously listening for
        incoming messages on a specified RabbitMQ queue without blocking an execution worker.
        Once a message arrives (and optionally passes processing), it yields a TriggerEvent.

        :param queue_name: The name of the RabbitMQ queue to monitor.
        :param conn_id: The Airflow connection id to use. Default is "rabbitmq_default".
        :param connection_uri: Optional direct AMQP connection URI (overrides conn_id if set).
        :param exchange_type: The type of exchange ("queue", "fanout", "direct", "topic").
        :param exchange_name: The name of the exchange to bind to.
        :param routing_key: The routing key used for binding the queue to the exchange.
        :param requeue_on_error: Whether to requeue the message if processing fails.
        :param poll_interval: Interval in seconds between reconnection attempts or polling loops.
        :param apply_function: Optional dot-notated string path to a callable used for processing messages.
        :param apply_function_args: Positional arguments passed to the apply_function.
        :param apply_function_kwargs: Keyword arguments passed to the apply_function.
        """
        super().__init__(**kwargs)  # type: ignore[no-untyped-call]
        self._connection: Optional[AbstractRobustConnection] = None
        self.conn_id = conn_id
        self.connection_uri = connection_uri
        self.queue_name = queue_name
        self.requeue_on_error = requeue_on_error
        self.poll_interval = poll_interval

        self.apply_function = apply_function
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}

        ex_type_str = str(exchange_type).lower() if exchange_type else "queue"

        if ex_type_str != "queue" and exchange_name:
            self.exchange_name = exchange_name
            self.exchange_type = exchange_type
            self.routing_key = routing_key or ""
        else:
            self.exchange_name = ""
            self.exchange_type = "queue"
            self.routing_key = "#"

        self.durable = kwargs.get("durable", True)
        self.exclusive = kwargs.get("exclusive", False)
        self.passive = kwargs.get("passive", False)
        self.auto_delete = kwargs.get("auto_delete", False)
        self.internal = kwargs.get("internal", False)
        self.timeout: Optional[int | float] = kwargs.get("timeout", None)
        self.arguments_queue: Optional[Dict[str, Any]] = kwargs.get(
            "arguments_queue", None
        )
        self.arguments_exchange: Optional[Dict[str, Any]] = kwargs.get(
            "arguments_exchange", None
        )
        self.arguments_bind: Optional[Dict[str, Any]] = kwargs.get(
            "arguments_bind", None
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.provider.rabbitmq.triggers.rabbitmq_trigger.RabbitMQTrigger",
            {
                "queue_name": self.queue_name,
                "conn_id": self.conn_id,
                "exchange_type": self.exchange_type,
                "exchange_name": self.exchange_name,
                "routing_key": self.routing_key,
                "requeue_on_error": self.requeue_on_error,
                "poll_interval": self.poll_interval,
                "apply_function": self.apply_function,
                "apply_function_args": self.apply_function_args,
                "apply_function_kwargs": self.apply_function_kwargs,
                "durable": self.durable,
                "exclusive": self.exclusive,
                "passive": self.passive,
                "auto_delete": self.auto_delete,
                "internal": self.internal,
                "timeout": self.timeout,
                "arguments_queue": self.arguments_queue,
                "arguments_exchange": self.arguments_exchange,
                "arguments_bind": self.arguments_bind,
            },
        )

    async def set_connection(self) -> None:
        if self.connection_uri is None:
            hook = RabbitMQHook(conn_id=self.conn_id)
        else:
            hook = RabbitMQHook(connection_uri=self.connection_uri)

        try:
            self._connection = await hook.get_async_connection()
        except Exception:
            if self.connection_uri is None:
                self.log.warning(
                    "\n==================================================\n"
                    "Error: Cannot connect to RabbitMQ!\n"
                    f"Conn ID: {self.conn_id}\n"
                    f"Connection URI: {hook.connection_uri}\n"
                    "==================================================\n"
                )
            else:
                self.log.warning(
                    "\n==================================================\n"
                    "Error: Cannot connect to RabbitMQ!\n"
                    f"Connection URI: {self.connection_uri}\n"
                    "==================================================\n"
                )
            await asyncio.sleep(self.poll_interval)
            self._connection = None

        if self.connection_uri is None:
            self.log.info(
                "\n==================================================\n"
                "SUKCES: Connected to RabbitMQ!\n"
                f"Conn ID: {self.conn_id}\n"
                f"Connection URI: {hook.connection_uri}\n"
                "==================================================\n"
            )
        else:
            self.log.info(
                "\n==================================================\n"
                "SUKCES: Connected to RabbitMQ!\n"
                f"Connection URI: {hook.connection_uri}\n"
                "==================================================\n"
            )

    async def declare_queue(
        self, channel: AbstractRobustChannel
    ) -> AbstractRobustQueue:
        queue = await channel.declare_queue(
            name=self.queue_name,
            durable=self.durable,
            exclusive=self.exclusive,
            passive=self.passive,
            auto_delete=self.auto_delete,
            arguments=self.arguments_queue,
            timeout=self.timeout,
        )

        if self.exchange_name != "":
            exchange = await channel.declare_exchange(
                name=self.exchange_name,
                type=self.exchange_type,
                durable=self.durable,
                auto_delete=self.auto_delete,
                internal=self.internal,
                passive=self.passive,
                arguments=self.arguments_exchange,
                timeout=self.timeout,
            )
            await queue.bind(
                exchange=exchange,
                routing_key=self.routing_key or "#",
                arguments=self.arguments_bind,
                timeout=self.timeout,
            )

        return queue

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            if self._connection is None:
                await self.set_connection()
                continue

            channel = await self._connection.channel()
            await channel.set_qos(prefetch_count=1)
            queue = await self.declare_queue(channel)

            async_message_process = None
            if self.apply_function:
                processing_call = import_string(self.apply_function)
                processing_call = partial(
                    processing_call,
                    *self.apply_function_args,
                    **self.apply_function_kwargs,
                )
                async_message_process = sync_to_async(processing_call)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if message is None:
                        self.log.info("Message is empty")
                        continue
                    event: Any = None
                    async with message.process(requeue=self.requeue_on_error):
                        if async_message_process:
                            event = await async_message_process(message)
                        else:
                            self.log.info("Requeued message: %s", message.body)
                            event = message.body.decode("utf-8")

                    if event:
                        yield TriggerEvent(event)  # type: ignore[no-untyped-call]
                        return
                    else:
                        await asyncio.sleep(self.poll_interval)

    async def cleanup(self) -> None:
        conn = self._connection
        if conn is not None:
            self._connection = None
            try:
                await conn.close()
            except Exception:
                self.log.warning(
                    "\n===================================================\n"
                    "Error: Cannot close RabbitMQ!\n"
                    "====================================================="
                )
