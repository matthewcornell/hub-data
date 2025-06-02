# hubdata

Python tools for accessing and working with hubverse Hub data

## running commands locally

All commands should be run from this repository's root, i.e., first do:

```bash
cd /<path_to_repos>/hub-data/
```

### app

Print a test hub's schema:

```bash
uv run hubdata schema test/hubs/example-complex-scenario-hub
```

### linter (ruff)

```bash
uv run ruff check
```

### tests (pytest)

```bash
uv run pytest
```

### coverage (coverage)

To generate a text report:

```bash
uv run --frozen coverage run -m pytest
uv run --frozen coverage report
rm .coverage
```

To generate a html report:

```bash
uv run --frozen coverage html
rm -rf htmlcov/index.html
```

### type checking (mypy)

```bash
uv tool run mypy . --ignore-missing-imports --disable-error-code=attr-defined
```

