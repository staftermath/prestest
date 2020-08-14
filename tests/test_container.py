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


def test_delete_remove_file_from_container_correctly(container, start_container, tmpdir):
    test_file = Path(tmpdir.join("test_delete.txt"))
    test_file.write_text("hello world")
    container_path = Path("/tmp") / "delete.txt"
    container.copy_from_local(test_file, container_path)
    # target method
    container.delete(container_path)

    with pytest.raises(RuntimeError):
        container.download_from_container(container_path, tmpdir.join("dummy_download.txt"))


@pytest.fixture()
def create_dummy_folders(request, tmpdir, container):
    test_folder = Path(tmpdir.join("test_folder"))
    test_folder.mkdir()
    (test_folder / "file1.txt").write_text("1")
    (test_folder / "file2.txt").write_text("2")

    def fin():
        container.delete("/tmp/test_folder")

    request.addfinalizer(fin)

    return test_folder


def test_copy_from_local_and_download_from_container_copy_file_correctly(container, start_container, create_dummy_folders,
                                                                         tmpdir):
    container_path = Path("/tmp")
    container.copy_from_local(create_dummy_folders, container_path)
    download_file = Path(tmpdir) / "test"
    container.download_from_container(container_path / "test_folder", download_file)
    files = [file.name for file in download_file.iterdir()]

    assert set(files) == {'file2.txt', 'file1.txt'}
    with open(download_file / 'file1.txt', 'r') as f:
        result1 = [l.strip() for l in f.readlines()]
        assert result1 == ['1']

    with open(download_file / 'file2.txt', 'r') as f:
        result2 = [l.strip() for l in f.readlines()]
        assert result2 == ['2']
