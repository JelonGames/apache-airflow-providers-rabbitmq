# Usage Guide: Apache Airflow RabbitMQ Provider

The RabbitMQ provider for Apache Airflow allows you to interact with RabbitMQ for publishing messages and sensing for messages in queues.

## Installation

```bash
pip install apache-airflow-provider-rabbitmq
```

## Connection Configuration

To use the RabbitMQ provider, you need to configure a connection in Airflow.

### Using Airflow Connection UI

1. Go to **Admin** -> **Connections**.
2. Click the **+** sign to add a new connection.
3. Set **Connection Id** to `rabbitmq_default` (or your preferred ID).
4. Set **Connection Type** to `RabbitMQ`.
5. Fill in the following fields:
    - **Host**: Your RabbitMQ host.
    - **Port**: Your RabbitMQ port (default is 5672).
    - **Login**: Your RabbitMQ username.
    - **Password**: Your RabbitMQ password.
    - **Schema**: Your RabbitMQ virtual host (vhost).
6. Alternatively, you can provide a `connection_uri` in the **Extra** field:
    ```json
    {
      "connection_uri": "amqp://user:password@host:port/vhost"
    }
    ```

## Operators

### RabbitMQProducerOperator

The `RabbitMQProducerOperator` is used to publish messages to a RabbitMQ exchange.

#### Parameters

- `message`: The message string to be sent. (Templated)
- `exchange`: The name of the RabbitMQ exchange. (Templated)
- `routing_key`: The routing key for the message. (Templated)
- `conn_id`: (Optional) Airflow connection ID. Default is `rabbitmq_default`. To use a raw URI, set `connection_uri` in the connection's Extra field.
- `use_async`: (Optional) Boolean flag to use asynchronous publishing via `aio-pika`. Default is `False`.

#### Example

```python
from airflow.provider.rabbitmq.operators.rabbitmq_producer import RabbitMQProducerOperator

publish_task = RabbitMQProducerOperator(
    task_id="publish_message",
    message="Hello RabbitMQ!",
    exchange="",
    routing_key="test_queue",
    conn_id="rabbitmq_default",
)
```

## Sensors

### RabbitMQSensor

The `RabbitMQSensor` waits for a message to appear in a RabbitMQ queue and pushes it to XCom.

#### Parameters

- `queue_name`: The name of the RabbitMQ queue to monitor. (Templated)
- `conn_id`: (Optional) Airflow connection ID. Default is `rabbitmq_default`. To use a raw URI, set `connection_uri` in the connection's Extra field.
- `auto_ack`: (Optional) Acknowledge the message, removing it from the queue. Default is `True`. With `False` the sensor peeks: it returns the message and requeues it.
- `deserialize`: (Optional) `None` returns the body as a string, or `"json"` parses it. A body that isn't valid JSON fails the task and stays on the queue. Default is `None`.
- `max_xcom_bytes`: (Optional) Fail the task, leaving the message on the queue, when the body is larger than this. Default is `None` (no limit).

#### XCom value

```python
{
    "body": "...",              # str, base64 str, or the parsed JSON value
    "body_encoding": "utf-8",   # "utf-8", "base64" (body wasn't UTF-8) or "json"
    "exchange": "orders",
    "routing_key": "new",
    "redelivered": False,
    "content_type": "application/json",
    "message_id": "...",
    "correlation_id": "...",
    "headers": {"source": "shop"},  # values converted to str
}
```

`RabbitMQTrigger` yields the same dict as its event payload.

The value is stored in the Airflow metadata database. If the queue can carry large messages, set `max_xcom_bytes`, or pass a reference (such as an object storage key) in the message instead of the data.

The body is logged at DEBUG only; INFO logs its size and routing key.

#### Example

```python
from airflow.provider.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor

wait_for_message = RabbitMQSensor(
    task_id="wait_for_message",
    queue_name="test_queue",
    deserialize="json",
    poke_interval=30,
    timeout=600,
)

# Downstream: ti.xcom_pull(task_ids="wait_for_message")["body"]["order_id"]
```

## Hooks

### RabbitMQHook

The `RabbitMQHook` provides a low-level interface to RabbitMQ.

```python
from airflow.provider.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook

hook = RabbitMQHook(conn_id="rabbitmq_default")
hook.publish_sync(message="Sync message", exchange="", routing_key="test_queue")
```
