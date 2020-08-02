from pathlib import Path
import pytest
import subprocess

from prestest.container import Container

DOCKER_FOLDER = Path(".").resolve().parent / "docker-hive"

@pytest.fixture()
def container() -> Container:
    return Container(docker_folder=DOCKER_FOLDER)


@pytest.fixture()
def start_container(container):
    container.start()


def test_start_stop_and_is_started(container):
    try:
        process = subprocess.Popen(f"docker-compose stop",
                                   cwd=DOCKER_FOLDER,
                                   shell=True,
                                   stdout=subprocess.PIPE)
        process.wait()
    except:
        pass

    container.start()
    result = container.is_started()
    assert result

    container.stop()
    result = container.is_started()
    assert not result


def test_is_presto_started_return_correct_value(container, start_container):
    result = container.is_presto_started()
    assert result
