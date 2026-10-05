#!/usr/bin/env bash
# Run kube.yaml with `podman kube play`.
#   ./kube.sh up     build the Airflow image and start the pod (replaces a running one)
#   ./kube.sh down   remove the pod; the next start is a fresh database
set -euo pipefail

example_dir="$(cd "$(dirname "$0")" && pwd)"
repo_dir="$(cd "$example_dir/../../.." && pwd)"

render() {
  sed -e "s|__EXAMPLE_DIR__|$example_dir|g" -e "s|__REPO_DIR__|$repo_dir|g" \
    "$example_dir/kube.yaml"
}

case "${1:-}" in
  up)
    podman build \
      --build-arg "AIRFLOW_VERSION=${AIRFLOW_VERSION:-3.3.2}" \
      --build-arg "PYTHON_VERSION=${PYTHON_VERSION:-3.12}" \
      -t localhost/rabbitmq-provider-airflow:local \
      -f "$example_dir/Containerfile" "$repo_dir"
    render | podman kube play --replace -
    ;;
  down)
    render | podman kube down -
    ;;
  *)
    echo "usage: $0 up|down" >&2
    exit 2
    ;;
esac
