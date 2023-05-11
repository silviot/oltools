from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa
from oltools.db import get_connection
from oltools.db import insert_from_file
from oltools.tests.utils import TEST_FILE_PATH
from oltools.tests.utils import TEST_FAULTY_FILE_PATH
import pytest


insert_options = [
    {"name": TEST_FILE_PATH, "count": 1000, "bytes": 147044},
    {"name": TEST_FAULTY_FILE_PATH, "count": 50, "bytes": 10867},
]


@pytest.mark.slow
@pytest.mark.parametrize("file", insert_options)
def test_insert(psql_service, file):  # noqa F811
    connection = get_connection(psql_service)
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    original_count = cursor.fetchone()[0]
    wrapper = SimpleWrapper()
    totals = {}

    def update_progress(category, advance):
        totals.setdefault(category, 0)
        totals[category] += advance

    insert_from_file(
        file["name"],
        psql_service,
        file_wrapper=wrapper,
        chunk_size=13,
        update_progress=update_progress,
    )
    assert totals["global"] == file["count"]
    assert wrapper.total_bytes == file["bytes"]
    assert wrapper.processed_bytes == file["bytes"]
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    assert cursor.fetchone()[0] == original_count + file["count"]
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
