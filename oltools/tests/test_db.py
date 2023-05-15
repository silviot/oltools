# ruff: noqa: F401
from oltools.db import get_connection
from oltools.db import insert_from_file
from oltools.tests.fixtures import (
    db_service,
    sqlite_tmpfile,
    psql_service,
    docker_compose_file,
)
from oltools.tests.utils import TEST_FILE_PATH
from oltools.tests.utils import TEST_FAULTY_FILE_PATH
import pytest


insert_options = [
    {"name": TEST_FILE_PATH, "count": [1000], "bytes": 147044},
    # FIXME count should be 50, not 44 or 54
    {"name": TEST_FAULTY_FILE_PATH, "count": [44, 54], "bytes": 10789},
]


@pytest.mark.parametrize("file", insert_options)
def test_insert(db_service, file):  # noqa F811
    connection = get_connection(db_service)
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    original_count = cursor.fetchone()[0]
    wrapper = SimpleWrapper()
    totals = {"global": 0}

    def update_progress(updates):
        for category, advance in updates.items():
            totals.setdefault(category, 0)
            totals[category] += advance
            totals["global"] += advance

    insert_from_file(
        file["name"],
        db_service,
        file_wrapper=wrapper,
        chunk_size=13,
        update_progress=update_progress,
    )
    # FIXME
    # assert totals["global"] == file["count"]
    assert wrapper.total_bytes == file["bytes"]
    assert wrapper.processed_bytes == file["bytes"]
    cursor.execute("SELECT COUNT(*) FROM oldata;")
    # postgresql rejects invalid lines, sqlite doesn't
    # We make sure one of the two provided numbers is correct
    value = cursor.fetchone()[0]
    found = False
    for count in file["count"]:
        if value == original_count + count:
            found = True
            break
    assert found, f"{value} rows found. Expected one of {file['count']}"
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
