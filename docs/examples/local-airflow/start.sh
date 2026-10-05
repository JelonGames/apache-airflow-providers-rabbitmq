#!/usr/bin/env bash
set -euo pipefail

# Under `podman kube play` nothing orders the containers, so wait for Postgres here.
until airflow db check >/dev/null 2>&1; do
  echo "Waiting for the metadata database..."
  sleep 2
done

airflow db migrate

# Create the connection only once, so edits made in the UI survive restarts.
# Schema (vhost) is left empty: the broker's default vhost "/" is used.
if ! airflow connections get rabbitmq_default >/dev/null 2>&1; then
  airflow connections add rabbitmq_default \
    --conn-type rabbitmq \
    --conn-host rabbitmq \
    --conn-port 5672 \
    --conn-login airflow \
    --conn-password airflow
fi

exec airflow standalone
