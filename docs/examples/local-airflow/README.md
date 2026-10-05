# Local Airflow for testing the provider

A Kubernetes Pod, run with `podman kube play`, with Airflow (this provider installed from your checkout), RabbitMQ and
Postgres, so you can try the operator, the sensor and the connection form in the Airflow UI.

| Container  | What it is                                                                         | URL                                            |
|------------|------------------------------------------------------------------------------------|------------------------------------------------|
| `airflow`  | Airflow 3.3.2 in standalone mode (API server, scheduler, DAG processor, triggerer) | http://localhost:8080                          |
| `rabbitmq` | RabbitMQ 4 with the management plugin                                              | http://localhost:15672 (`airflow` / `airflow`) |
| `postgres` | Airflow's metadata database                                                        |                                                |

The Airflow UI has no login: every visitor is an admin.

## Start

From this directory:

```bash
./kube.sh up
```

The first build pulls the Airflow image and takes a few minutes. Airflow is ready when http://localhost:8080 loads.
No cluster is needed: `kube.sh` builds the image with `podman build` and starts `kube.yaml` with `podman kube play`.

To pin another Airflow or Python version:

```bash
AIRFLOW_VERSION=3.1.7 PYTHON_VERSION=3.11 ./kube.sh up
```

The three containers share the Pod's network. `hostAliases` map `rabbitmq` and `postgres` to localhost, so the
connection can point at host `rabbitmq`.

## What's set up

- **Connection** `rabbitmq_default`, pointing at host `rabbitmq` as user `airflow`. It's created once in the
  metadata database, so it shows under Admin > Connections and your edits survive restarts.
- **Broker topology**, loaded from `rabbitmq/definitions.json`:
    - queues `my_queue`, `demo_queue`, `demo_exchange_queue`
    - direct exchange `demo_exchange`, routing key `demo` to `demo_exchange_queue`
- **DAGs**:
    - `rabbitmq_local_demo` (`dags/rabbitmq_local_demo.py`) publishes through the default exchange and through
      `demo_exchange`, both sync and async, and waits for each message with the sensor.
    - `rabbitmq_example_dag` is `docs/examples/rabbitmq_dag.py`, mounted as is.

DAGs start paused. Unpause one and trigger it from the UI.

## Changing the provider

`src/` is mounted into the container, and the provider is installed from it in editable mode. After editing the code:

```bash
podman restart rabbitmq-provider-local-airflow
```

Changes to `pyproject.toml` (dependencies, entry points) need a rebuild: `./kube.sh up`. It replaces the pod, so the database starts fresh.

Logs: `podman logs -f rabbitmq-provider-local-airflow`.

## Stop

```bash
./kube.sh down                           # remove the pod: the next start is a fresh database
podman pod stop rabbitmq-provider-local  # keep connections and run history; `podman pod start` resumes
```

## Notes

- The connection leaves Schema (vhost) empty, so the broker's default vhost `/` is used. The hook doesn't URL-encode the
  vhost yet, so avoid typing `/` into that field (#7).
- The management UI at http://localhost:15672 is the quickest way to see what the operator published or what the sensor
  consumed.
