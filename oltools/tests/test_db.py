from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa
from oltools.parsers import stream_file
from oltools.tests.utils import get_test_fh
import psycopg
import pytest


@pytest.mark.slow
def test_insert(psql_service):  # noqa F811
    connection = psycopg.connect(psql_service)
    with get_test_fh() as fh:
        i = 0
        for line in stream_file(fh, "bz2"):
            i += 1
        assert i == 1000

    print(connection)
