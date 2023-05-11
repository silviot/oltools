from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa
from oltools.db import get_connection
from oltools.db import insert_from_file
from oltools.tests.utils import TEST_FILE_PATH
import pytest


@pytest.mark.slow
def test_insert(psql_service):  # noqa F811
    connection = get_connection(psql_service)
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    original_count = cursor.fetchone()[0]
    insert_from_file(TEST_FILE_PATH, psql_service)
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    assert cursor.fetchone()[0] == original_count + 1000
    connection.close()
