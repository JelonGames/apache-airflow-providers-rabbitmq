## What this changes and why

<!-- The problem this solves and the approach taken. Link the issue if there is one (Fixes #123). -->

## How it was tested

<!-- Unit/integration tests added or run, and which tox environments (Airflow 2.x and 3.x if the change touches Airflow APIs). -->

## Checklist

- [ ] Tests pass locally (`uv run pytest tests/unit/`)
- [ ] `black`, `isort`, `flake8` and `mypy` pass
- [ ] `provider.yaml` and `get_provider_info()` updated if a component or connection field changed
- [ ] `RELEASE_NOTES.md` updated if users will see the change
- [ ] Commit subjects follow Conventional Commits (`feat:`, `fix:`, `feat!:` for breaking changes)
