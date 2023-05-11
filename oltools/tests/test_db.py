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
    wrapper = SimpleWrapper()
    insert_from_file(TEST_FILE_PATH, psql_service, file_wrapper=wrapper)
    assert wrapper.total_bytes == 147044
    assert wrapper.processed_bytes == 147044
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    assert cursor.fetchone()[0] == original_count + 1000
    connection.close()


class SimpleWrapper:
    """Simple wrapper to test the wrapper parameter of inser_from_file"""

    def __call__(self, fh, total, description=""):
        assert not hasattr(self, "fh")
        self.total_bytes = total
        self.fh = fh
        self.processed_bytes = 0
        return self

    def read(self, *args, **kwargs):
        data = self.fh.read(*args, **kwargs)
        self.processed_bytes += len(data)
        return data
