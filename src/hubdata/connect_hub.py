import json
from pathlib import Path


def connect_hub(hub_path: Path):
    """
    The main entry point for connecting to a hub. This will connect to data in a model output directory through a
    Modeling Hub or directly. Data can be stored in a local directory or in the cloud on AWS or GCS.

    :param hub_path: Path to a hub's root directory. see: https://docs.hubverse.io/en/latest/user-guide/hub-structure.html
    :return: a HubConnection
    :raise: RuntimeError if `hub_path` is not found
    """
    return HubConnection(hub_path)


class HubConnection:
    """
    Provides convenient access to various parts of a hub's `tasks.json` file. Use the `connect_hub` factory function to
    create instances of this class, rather than by direct instantiation.

    Instance variables:
    - hub_dir: Path to a hub's root directory. see: https://docs.hubverse.io/en/latest/user-guide/hub-structure.html
    - tasks: the hub's `tasks.json` contents
    - model_metadata_schema: the hub's `model-metadata-schema.json` contents
    """


    def __init__(self, hub_dir: Path):
        """
        :param hub_dir: Path to a hub's root directory. see: https://docs.hubverse.io/en/latest/user-guide/hub-structure.html
        """

        self.hub_dir = hub_dir
        if not self.hub_dir.exists():
            raise RuntimeError(f'hub_dir not found: {self.hub_dir}')

        with open(self.hub_dir / 'hub-config' / 'tasks.json') as fp:
            self.tasks: dict = json.load(fp)

        with open(self.hub_dir / 'hub-config' / 'model-metadata-schema.json') as fp:
            self.model_metadata_schema: dict = json.load(fp)
