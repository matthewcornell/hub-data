import json
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import structlog

from hubdata.create_hub_schema import create_hub_schema

logger = structlog.get_logger()


def connect_hub(hub_path: Path):
    """
    The main entry point for connecting to a hub. This will connect to data in a model output directory through a
    Modeling Hub or directly. Data can be stored in a local directory or in the cloud on AWS or GCS. Calls
    `create_hub_schema()` to get the schema to use when calling `HubConnection.get_dataset()`.

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
    - hub_path: Path to a hub's root directory. see: https://docs.hubverse.io/en/latest/user-guide/hub-structure.html
    - schema: the pa.Schema for `HubConnection.get_dataset()`. created via `create_hub_schema()`
    - admin: the hub's `admin.json` contents as a dict
    - tasks: "" `tasks.json` ""
    - model_output_dir: Path to the hub's model output directory
    """


    def __init__(self, hub_path: Path):
        """
        :param hub_path: Path to a hub's root directory. see: https://docs.hubverse.io/en/latest/user-guide/hub-structure.html
        """

        # set hub_path, first checking for existence
        self.hub_path = hub_path
        if not self.hub_path.exists():
            raise RuntimeError(f'hub_path not found: {self.hub_path}')

        # set self.admin, first checking for admin.json existence
        admin_json_file = self.hub_path / 'hub-config' / 'admin.json'
        if not admin_json_file.exists():
            raise RuntimeError(f'admin.json not found: {admin_json_file}')

        with open(admin_json_file) as fp:
            self.admin: dict = json.load(fp)

        # set self.tasks, first checking for tasks.json existence
        tasks_json_file = self.hub_path / 'hub-config' / 'tasks.json'
        if not tasks_json_file.exists():
            raise RuntimeError(f'tasks.json not found: {tasks_json_file}')

        with open(tasks_json_file) as fp:
            self.tasks: dict = json.load(fp)

        # set schema
        self.schema = create_hub_schema(self.tasks)

        # set self.model_output_dir, first checking for directory existence
        model_output_dir_name = self.admin['model_output_dir'] if 'model_output_dir' in self.admin else 'model-output'
        model_output_dir = Path(hub_path / model_output_dir_name)
        if not model_output_dir.exists():
            logger.warn(f'model_output_dir not found: {model_output_dir!r}')
        self.model_output_dir = model_output_dir


    def get_dataset(self) -> ds:
        """
        :return: a pyarrow.dataset.Dataset for my model_output_dir
        """
        return self._get_path_dataset()


    def _get_path_dataset(self) -> ds:
        """
        :return: a pyarrow.dataset.UnionDataset with one child pyarrow.dataset.FileSystemDataset for each file_format in
        self.admin.
        """
        # create the dataset. NB: we are using dataset "directory partitioning" to automatically get the `model_id`
        # column from directory names. NB: each dataset is a pyarrow._dataset.FileSystemDataset (so far!)
        datasets = [ds.dataset(self.model_output_dir, format=file_format,
                               partitioning=['model_id'],  # NB: hard-coded partitioning!
                               exclude_invalid_files=True, schema=self.schema)
                    for file_format in self.admin['file_format']]
        return ds.dataset([dataset for dataset in datasets
                           if isinstance(dataset, pa.dataset.FileSystemDataset) and (len(dataset.files) != 0)])


    def to_table(self, *args, **kwargs) -> pa.Table:
        """
        A helper function that simply passes args and kwargs to `pyarrow.dataset.Dataset.to_table()`, returning the
        `pyarrow.Table`.
        """
        return self.get_dataset().to_table(*args, **kwargs)
