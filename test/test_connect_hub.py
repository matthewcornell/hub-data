from pathlib import Path

import pytest

from hubdata import connect_hub


def test_hub_dir_existence():
    with pytest.raises(RuntimeError, match='hub_dir not found'):
        connect_hub(Path('test/hubs/example-complex-forecast-hub') / 'nonexistent-dir')


def test_hub_fields():
    hub_dir = Path('test/hubs/example-complex-scenario-hub')
    hub_connection = connect_hub(hub_dir)
    assert hub_connection.hub_dir == hub_dir

    # spot-check tasks
    assert list(hub_connection.tasks.keys()) == ['schema_version', 'rounds']
    assert len(hub_connection.tasks['rounds']) == 4

    # spot-check model_metadata_schema
    assert (list(hub_connection.model_metadata_schema.keys()) ==
            ['$schema', 'title', 'description', 'type', 'properties', 'additionalProperties', 'required'])
