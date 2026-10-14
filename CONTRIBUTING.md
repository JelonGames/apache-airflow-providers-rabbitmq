# Contributing

Bug reports, fixes and features are welcome. This file covers what a change needs before it can be merged. Everyone taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting bugs and asking for features

Open an [issue](https://github.com/mustafa-zidan/apache-airflow-providers-rabbitmq/issues/new/choose) using one of the templates. For a bug, include the provider, Airflow, Python and RabbitMQ versions, and the smallest DAG or snippet that reproduces it. Most bugs here depend on the Airflow version, so a report without one usually comes back with a question.

Security problems go through [SECURITY.md](SECURITY.md), not a public issue.

## Development setup

You need Python 3.11+, [uv](https://docs.astral.sh/uv/), and Docker for the integration tests.

```bash
git clone https://github.com/mustafa-zidan/apache-airflow-providers-rabbitmq.git
cd apache-airflow-providers-rabbitmq
uv sync --extra development
```

The package lives under `src/airflow/provider/rabbitmq` (singular `provider`). The `airflow.providers.rabbitmq` path only holds backward-compatibility shims; new code doesn't go there.

## Tests

```bash
uv run pytest tests/unit/
uv run pytest tests/integration/   # starts RabbitMQ in Docker via testcontainers
```

CI runs the unit tests across every supported Python and Airflow combination through tox, plus integration tests on the latest Airflow 2.x and 3.x. To run one of those environments locally:

```bash
uv run tox -e py312-airflow31
uv run tox -e py312-airflow31-integration
```

The full list of environments is in `tox.ini`. A change that touches Airflow APIs that moved between 2.x and 3.x should be checked on at least one environment from each.

## Linting and types

Install the git hooks once after cloning:

```bash
uv run pre-commit install
```

Every commit then runs black, isort, flake8 and mypy (plus whitespace and YAML/TOML checks) on the files it touches. The tools run from the uv environment, so their versions come from `uv.lock`. The lint workflow runs the same hooks on every file, which you can do locally with:

```bash
uv run pre-commit run --all-files
```

mypy runs in strict mode, so new functions need full type annotations.

## Provider metadata

`provider.yaml` and `get_provider_info()` describe the same provider, and a test checks that they agree. If you add a hook, operator, sensor, trigger or connection field, register it in both.

## Commits and pull requests

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`), with `!` after the type for a breaking change, for example `refactor!: rename RabbitMQSensor queue argument to queue_name`. The release notes are written from these subjects.

Open the pull request against `main`. In the description, say what problem the change solves and link the issue if there is one. If it changes behavior users see, add an entry under an unreleased section at the top of `RELEASE_NOTES.md`; breaking changes also need a before/after snippet in the migration guide there.

## License

Contributions are licensed under the [Apache License 2.0](LICENSE), the same as the rest of the project.
