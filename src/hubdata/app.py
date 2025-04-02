import structlog
from rich.console import Console
from rich.panel import Panel

from hubdata.logging import setup_logging

setup_logging()
logger = structlog.get_logger()


def main():
    """hubdata starting point."""
    logger.info('starting hubdata...')

    console = Console()
    console.print(
        Panel(
            ':tada: Hello from the hubdata Python package!',
            border_style='green',
            expand=False,
            padding=(1, 4),
            subtitle='[italic]created by pyprefab[/italic]',
            subtitle_align='right',
            title='hubdata',
            title_align='left',
        )
    )


if __name__ == '__main__':
    main()

