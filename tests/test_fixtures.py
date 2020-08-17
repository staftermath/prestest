import pytest
from pathlib import Path

from prestest.fixtures import *

resource_folder = Path(".").resolve() / "resources"


@pytest.mark.container_folder(resource_folder)
def test_container_set_docker_folder_correctly(container):
    assert container.docker_folder == resource_folder


@pytest.mark.enable_table_modification
def test_start_container_enable_table_modification_correctly(start_container, container, tmpdir):
    temp_download = Path(tmpdir.join("test_start_container_enable_table_modification_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert result.issuperset(expected)


@pytest.mark.reset
def test_start_container_reset_correctly(start_container, container, tmpdir):
    temp_download = Path(tmpdir.join("test_start_container_enable_table_modification_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert not result.intersection(expected)
