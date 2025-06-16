# hubdata

Python tools for accessing and working with hubverse Hub data

## Basic usage

> Note: This package is based on the [python version](https://arrow.apache.org/docs/python/index.html) of Apache's [Arrow library](https://arrow.apache.org/docs/index.html).

1. Use `connect_hub()` to get a `HubConnection` object for a local hub directory. (NB: Currently we only support connecting to local hub directories and not S3 hubs. We anticipate adding support for the latter shortly.)
2. Call `HubConnection.get_dataset()` to get a pyarrow [Dataset](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html) for the hub's model output directory.
3. Work with the data by calling functions directly on the dataset, or use [Dataset.to_table()](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.to_table) to get read the data into a https://arrow.apache.org/docs/python/generated/pyarrow.Table.html . You can use pyarrow's [compute functions](https://arrow.apache.org/docs/python/compute.html) or convert the table to another format, such as [polars](https://docs.pola.rs/api/python/dev/reference/api/polars.from_arrow.html) or [pandas](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas).

For example, here is code to count the number of rows total in the `test/hubs/flu-metrocast` test hub.

First, start a python interpreter with the required libraries:

```bash
cd /<path_to_repos>/hub-data/
uv run python3
```

Then run this example code:

```python
from pathlib import Path
from hubdata import connect_hub


hub_connection = connect_hub(Path('test/hubs/flu-metrocast'))
hub_ds = hub_connection.get_dataset()
hub_ds.count_rows()
```

The output should be `14895`.

## Run commands locally

Following are some useful local commands for testing and trying out the package's features. The pacakge follows the [Hubverse Python package standard](https://docs.hubverse.io/en/latest/developer/python.html), and in particular uses [uv](https://docs.astral.sh/uv/) for managing Python versions, virtual environments, and dependencies.

Note that all commands should be run from this repository's root, i.e., first do:

```bash
cd /<path_to_repos>/hub-data/
```

### app (hubdata)

The package provides a CLI called `hubdata` (defined in `pyproject.toml`'s "project.scripts" table). Here's an example of running the command to print a test hub's schema and its dataset info:

```bash
uv run hubdata schema test/hubs/flu-metrocast
uv run hubdata dataset test/hubs/flu-metrocast
```

### tests (pytest)

Use this command to run tests via [pytest](https://docs.pytest.org/en/stable/):

```bash
uv run pytest
```

### linter (ruff)

Run this command to invoke the [ruff](https://github.com/astral-sh/ruff) code formatter.

```bash
uv run ruff check
```

### coverage (coverage)

Run this command to generate a _text_ [coverage](https://coverage.readthedocs.io/en/7.8.2/) report:

```bash
uv run --frozen coverage run -m pytest
uv run --frozen coverage report
rm .coverage
```

This command generates an _html_ report:

```bash
uv run --frozen coverage html
rm -rf htmlcov/index.html
```

### type checking (mypy)

Use this command to do some optional static type checking using [mypy](https://mypy-lang.org/):

```bash
uv tool run mypy . --ignore-missing-imports --disable-error-code=attr-defined
```
