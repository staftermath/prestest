from pathlib import Path
import pytest
import subprocess
import time

from docker.errors import APIError, NotFound

from prestest.container import Container, CONTAINER_NAMES, LOCAL_FILE_STORE_NODE

DOCKER_FOLDER = Path(".").resolve().parent / "docker-hive"


@pytest.fixture()
def container() -> Container:
    return Container(docker_folder=DOCKER_FOLDER)


@pytest.fixture()
def start_container(container):
    container.start(until_started=True)


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

    container.start(until_started=False)
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


def test_upload_temp_table_file_returned_context_manager_working_properly(container, start_container, tmpdir):
    tempfile = Path(tmpdir.join("test_context_manager.txt"))
    tempfile.write_text("hello word")

    downloaded_file = Path(tmpdir.join("test_context_manager_downloaded.txt"))
    with container.upload_temp_table_file(tempfile) as uploaded_f:
        container.download_from_container(uploaded_f, downloaded_file)
        with open(downloaded_file, 'r') as downloaded_f:
            l = downloaded_f.readline()
            assert l == "hello word", "file not properly uploaded by context manager"

    with pytest.raises(RuntimeError):
        container.download_from_container(uploaded_f, tmpdir.join("should_not_be_downloaded"))


def test_append_file_add_line_correctly(container, start_container, tmpdir):
    test_line = "hello world"
    tempfile = Path(tmpdir.join("test_edit_file.txt"))
    tempfile.write_text(test_line)
    with container.upload_temp_table_file(tempfile) as uploaded_f:
        container.append_file(CONTAINER_NAMES[LOCAL_FILE_STORE_NODE], uploaded_f, test_line, user='1000')
        temp_download = Path(tmpdir.join("test_edit_file_downloaded.txt"))
        container.download_from_container(uploaded_f, temp_download)
        with open(temp_download, 'r') as download_f:
            lines = [l.strip() for l in download_f.readlines()]
            assert lines == [test_line], "should not change content if line already exists"

        container.append_file(CONTAINER_NAMES[LOCAL_FILE_STORE_NODE], uploaded_f, "this is a new line", user='1000')
        container.download_from_container(uploaded_f, temp_download)
        with open(temp_download, 'r') as download_f:
            lines = [l.strip() for l in download_f.readlines()]
            assert lines == [test_line, "this is a new line"], "should append line when it doesn't exists"


@pytest.fixture()
def reset_container(request, container):
    try:
        container.reset()
    except NotFound:
        pass

    container.start()
    def fin():
        container.reset()

    request.addfinalizer(fin)


def test_enable_table_modification_change_hive_properties_file_correctly(container, reset_container, tmpdir):
    container.enable_table_modification()
    temp_download = Path(tmpdir.join("temp_download_hive_properties"))
    container.download_from_container(from_container="/opt/presto-server-0.181/etc/catalog/hive.properties",
                                      to_local=temp_download,
                                      container_name=CONTAINER_NAMES["presto_coordinator"])
    with open(temp_download, 'r') as f:
        result = set(l.strip() for l in f.readlines() if l.strip() != '')
    expected = {"hive.allow-drop-table=true", "hive.allow-rename-table=true", "hive.allow-add-column=true"}
    assert result.issuperset(expected)
