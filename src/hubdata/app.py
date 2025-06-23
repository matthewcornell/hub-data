import pathlib

import click
import pyarrow as pa
import structlog
from rich.console import Console, Group
from rich.panel import Panel

from hubdata import connect_hub
from hubdata.logging import setup_logging

setup_logging()
logger = structlog.get_logger()


@click.group()
def cli():
    pass


@cli.command(name='schema')
@click.argument('hub_path', type=click.Path(file_okay=False, exists=True, path_type=pathlib.Path))
def print_schema(hub_path):
    """
    A subcommand that prints the output of `create_hub_schema()` for `hub_path`.
    """
    hub_connection = connect_hub(hub_path)

    # create the hub_path group lines
    hub_path_lines = ['[b]hub_path[/b]:',
                     f'- {hub_path}']

    # create the schema group lines
    schema_lines = ['\n[b]schema[/b]:']
    for field in sorted(hub_connection.schema, key=lambda _: _.name):  # sort schema fields by name for consistency
        schema_lines.append(f'- [green]{field.name}[/green]: [bright_magenta]{field.type}[/bright_magenta]')

    # finally, print a Panel containing all the groups
    console = Console()
    console.print(
        Panel(
            Group(Group(*hub_path_lines), Group(*schema_lines)),
            border_style='green',
            expand=False,
            padding=(1, 2),
            subtitle='[italic]hubdata[/italic]',
            subtitle_align='right',
            title=f'[bright_red]{hub_path.name}[/bright_red]',
            title_align='left')
    )


@cli.command(name='dataset')
@click.argument('hub_path', type=click.Path(file_okay=False, exists=True, path_type=pathlib.Path))
def print_dataset_info(hub_path):
    """
    A subcommand that prints dataset information for `hub_path`. Currently only works with a UnionDataset of
    FileSystemDatasets.
    """
    hub_connection = connect_hub(hub_path)
    hub_ds = hub_connection.get_dataset()
    if not isinstance(hub_ds, pa.dataset.UnionDataset):
        print(f'sorry, currently only supports pa.dataset.UnionDataset, not {type(hub_ds)}')
        return

    # create the hub_path group lines
    hub_path_lines = ['[b]hub_path[/b]:',
                     f'- {hub_path}']

    # create the schema group lines
    schema_lines = ['\n[b]schema[/b]:']
    for field in sorted(hub_connection.schema, key=lambda _: _.name):  # sort schema fields by name for consistency
        schema_lines.append(f'- [green]{field.name}[/green]: [bright_magenta]{field.type}[/bright_magenta]')

    # create the dataset group lines
    num_files = sum([len(child_ds.files) for child_ds in hub_ds.children])
    file_types = ', '.join([child_ds.format.default_extname for child_ds in hub_ds.children])
    num_rows = hub_ds.count_rows()
    dataset_lines = ['\n[b]dataset[/b]:',
                     f'- [green]files[/green]: [bright_magenta]{num_files:,}[/bright_magenta]',
                     f'- [green]types[/green]: [bright_magenta]{file_types}[/bright_magenta]',
                     f'- [green]rows[/green]: [bright_magenta]{num_rows:,}[/bright_magenta]']

    # finally, print a Panel containing all the groups
    console = Console()
    console.print(
        Panel(
            Group(Group(*hub_path_lines), Group(*schema_lines), Group(*dataset_lines)),
            border_style='green',
            expand=False,
            padding=(1, 2),
            subtitle='[italic]hubdata[/italic]',
            subtitle_align='right',
            title=f'[bright_red]{hub_path.name}[/bright_red]',
            title_align='left')
    )


if __name__ == '__main__':
    cli()
