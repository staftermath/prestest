from pathlib import Path
import pytest
import subprocess
import time

from docker.errors import APIError

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


@pytest.mark.slow
def test_is_healthy_return_correct_value(container):
    try:
        process = subprocess.Popen(f"docker-compose stop",
                                   cwd=DOCKER_FOLDER,
                                   shell=True,
                                   stdout=subprocess.PIPE)
        process.wait()
    except:
        pass

    result = container.is_healthy()
    assert not result, "should not be healthy when not started"

    container.start()
    result = container.is_healthy()# If test is paused (for example, debug), this may fail
    assert not result, "should not be healthy when just started"

    wait_time = 60  # wait at most 60 secs
    sleep = 5
    while not result and wait_time > 0:
        time.sleep(sleep)
        wait_time -= sleep
        result = container.is_healthy()

    assert result, "should be healthy in 60 seconds"


def test_is_presto_started_return_correct_value(container, start_container):
    result = container.is_presto_started()
    assert result


def test_reset_remove_container_correctly(container, start_container):
    assert container.is_started(), "container not properly started before test"

    container.reset()

    with pytest.raises(APIError):
        container.is_started()
