# Tests for Apache Airflow Provider for RabbitMQ

This directory contains tests for the Apache Airflow Provider for RabbitMQ.

## Test Structure

The tests are organized as follows:

- `unit/`: Unit tests for individual components
  - `hooks/`: Tests for RabbitMQ hooks
  - `operators/`: Tests for RabbitMQ operators
  - `sensors/`: Tests for RabbitMQ sensors
- `integration/`: Integration tests that test the interaction between components
- `conftest.py`: Common fixtures and configuration for tests

## Running Tests

### Prerequisites

- Python 3.11 or later
- Apache Airflow 2.8.0 or later
- RabbitMQ server (for integration tests)

### Install Test Dependencies

```bash
uv sync --extras development
```

### Running Tests with pytest

To run all tests:

```bash
pytest
```

To run unit tests only:

```bash
pytest tests/unit/
```

To run a specific test file:

```bash
pytest tests/unit/hooks/test_rabbitmq_hook.py
```

To run a specific test:

```bash
pytest tests/unit/hooks/test_rabbitmq_hook.py::TestRabbitMQHook::test_init
```

To run tests with coverage:

```bash
pytest --cov=airflow.providers.rabbitmq
```

To generate a coverage report:

```bash
pytest --cov=airflow.providers.rabbitmq --cov-report=html
```

## Writing Tests

When writing tests for this provider, please follow these guidelines:

1. Use the fixtures provided in `conftest.py` when possible
2. Mock external dependencies (e.g., RabbitMQ connections)
3. Write both positive and negative test cases
4. Test error handling
5. Keep tests independent and idempotent
