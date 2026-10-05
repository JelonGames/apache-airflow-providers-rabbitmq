"""Compatibility shim for the legacy namespace."""

# Compatibility shim — redirects to new canonical namespace airflow.provider.rabbitmq
from airflow.provider.rabbitmq.version import __version__  # noqa: F401

__all__ = ["__version__"]
