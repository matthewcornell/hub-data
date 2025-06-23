# hubdata

Python tools for accessing and working with hubverse Hub data

## Basic usage

> Note: This package is based on the [python version](https://arrow.apache.org/docs/python/index.html) of Apache's [Arrow library](https://arrow.apache.org/docs/index.html).

1. Use `connect_hub()` to get a `HubConnection` object for a local hub directory. (NB: Currently we only support connecting to local hub directories and not S3 hubs. We anticipate adding support for the latter shortly.)
2. Call `HubConnection.get_dataset()` to get a pyarrow [Dataset](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html) for the hub's model output directory.
3. Work with the data by calling functions directly on the dataset, or use [Dataset.to_table()](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.to_table) to read the data into a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) . You can use pyarrow's [compute functions](https://arrow.apache.org/docs/python/compute.html) or convert the table to another format, such as [Polars](https://docs.pola.rs/api/python/dev/reference/api/polars.from_arrow.html) or [pandas](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas).

For example, here is code using native pyarrow commands to count the number of rows total in the `test/hubs/flu-metrocast` test hub, and then to get the unique locations in the dataset as a python list.

First, start a python interpreter with the required libraries:

```bash
cd /<path_to_repos>/hub-data/
uv run python3
```

Then run the following Python code. (Note that we've included Python output in comments.)

```python
from pathlib import Path
from hubdata import connect_hub
import pyarrow.compute as pc


hub_connection = connect_hub(Path('test/hubs/flu-metrocast'))
hub_ds = hub_connection.get_dataset()
hub_ds.count_rows()
# 14895

pc.unique(hub_ds.to_table()['location']).to_pylist()
# ['Bronx', 'Brooklyn', 'Manhattan', 'NYC', 'Queens', 'Staten Island', 'Austin', 'Dallas', 'El Paso', 'Houston', 'San Antonio']
```

## Memory considerations for large datasets

As mentioned above, we use the pyarrow [Dataset.to_table()](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.to_table) function to load a dataset into a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) . For example, continuing the above Python session:

```python
# naive approach to getting a table: load entire dataset into memory
pa_table = hub_ds.to_table()

print(pa_table.shape)
# (14895, 9)

print(pa_table.column_names)
# ['reference_date', 'target', 'horizon', 'location', 'target_end_date', 'output_type', 'output_type_id', 'value', 'model_id']
```

However, that function reads the entire dataset into memory, which could be unnecessary or fail for large hubs. A more parsimonious approach is to use the [Dataset.to_table()](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.to_table) `columns` and `filter` arguments to select and filter only the information of interest and limit what data is pulled into memory:

```python
# more parsimonious approach: load a subset of the data into memory (select only `target_end_date` and `value` associated with `Bronx` as location)
pa_table = hub_ds.to_table(columns=['target_end_date', 'value'],
                           filter=pc.field('location') == 'Bronx')

print(pa_table.shape)
# (1350, 2)
```

## Working with data outside pyarrow: A Polars example

As mentioned above, once you have a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) you can convert it to work with dataframe packages like [pandas](https://pandas.pydata.org/) and [Polars](https://docs.pola.rs/). Here we give an example of using the latter with a larger hub.

First, clone the https://github.com/cdcepi/FluSight-forecast-hub repository. This repo takes ~1.1GB of disk space and has ~2400 csv & parquet files totalling ~13M rows. (Counts are via the `hubdata dataset` command described above.)

```bash
cd /<path_to_repos>/ # Path to folder that will host the clone example repository
git clone https://github.com/cdcepi/FluSight-forecast-hub
```

Then start a python session, installing the Polars package on the fly using `uv run`'s [--with argument](https://docs.astral.sh/uv/concepts/projects/run/#requesting-additional-dependencies):

```bash
uv run --with polars python3
```

Finally, run the following Python commands to see Polars integration in action:

```python
from pathlib import Path

import polars as pl
import pyarrow.compute as pc
from hubdata import connect_hub


# connect to the hub and get a pyarrow Dataset
hub_path = Path('/<path_to_repos>/FluSight-forecast-hub')
hub_connection = connect_hub(hub_path)
hub_ds = hub_connection.get_dataset()  # can take a minute for pyarrow to scan files

# load the dataset into a pyarrow Table, limiting the columns and rows loaded into memory as described above
pa_table = hub_ds.to_table(columns=['target_end_date', 'value', 'output_type', 'output_type_id', 'reference_date'],
                           filter=(pc.field('location') == 'US') & (pc.field('target') == 'wk inc flu hosp'))

pa_table.shape
# (264645, 2)

# convert to polars DataFrame
pl_df = pl.from_arrow(pa_table) 
pl_df

# it's also possible to convert to a polars DataFrame and do some operations
pl_df = (
    pl.from_arrow(pa_table)
    .group_by(pl.col('target_end_date'))
    .agg(pl.col('value').count())
)

pl_df
# shape: (69, 2)
# ┌─────────────────┬───────┐
# │ target_end_date ┆ value │
# │ ---             ┆ ---   │
# │ str             ┆ u32   │
# ╞═════════════════╪═══════╡
# │ 2025-05-24      ┆ 5242  │
# │ 2023-09-30      ┆ 28    │
# │ 2025-05-31      ┆ 4907  │
# │ 2023-11-25      ┆ 3584  │
# │ 2024-03-23      ┆ 3943  │
# │ …               ┆ …     │
# │ 2025-04-05      ┆ 5542  │
# │ 2024-05-18      ┆ 1644  │
# │ 2023-11-04      ┆ 3341  │
# │ 2025-03-01      ┆ 5199  │
# │ 2025-06-07      ┆ 3620  │
# └─────────────────┴───────┘
```

## Run commands locally

Following are some useful local commands for testing and trying out the package's features. The package follows the [Hubverse Python package standard](https://docs.hubverse.io/en/latest/developer/python.html), and in particular uses [uv](https://docs.astral.sh/uv/) for managing Python versions, virtual environments, and dependencies.

Note that all commands should be run from this repository's root, i.e., first do:

```bash
cd /<path_to_repos>/hub-data/
```

### app (hubdata)

The package provides a CLI called `hubdata` (defined in `pyproject.toml`'s "project.scripts" table). Here's an example of running the command to print a test hub's schema and its dataset info:

```bash
uv run hubdata schema test/hubs/flu-metrocast
╭─ flu-metrocast ─────────────╮
│                             │
│  hub_path:                  │
│  - test/hubs/flu-metrocast  │
│                             │
│  schema:                    │
│  - horizon: int32           │
│  - location: string         │
│  - model_id: string         │
│  - output_type: string      │
│  - output_type_id: double   │
│  - reference_date: date32   │
│  - target: string           │
│  - target_end_date: date32  │
│  - value: double            │
│                             │
╰─────────────────── hubdata ─╯

uv run hubdata dataset test/hubs/flu-metrocast
╭─ flu-metrocast ─────────────╮
│                             │
│  hub_path:                  │
│  - test/hubs/flu-metrocast  │
│                             │
│  schema:                    │
│  - horizon: int32           │
│  - location: string         │
│  - model_id: string         │
│  - output_type: string      │
│  - output_type_id: double   │
│  - reference_date: date32   │
│  - target: string           │
│  - target_end_date: date32  │
│  - value: double            │
│                             │
│  dataset:                   │
│  - files: 31                │
│  - rows: 14,895             │
│                             │
╰─────────────────── hubdata ─╯
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
