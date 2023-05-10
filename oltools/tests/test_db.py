from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa
from oltools.parsers import stream_file
from oltools.parsers import stream_objects
from oltools.db import get_connection
from oltools.tests.utils import get_test_fh
import pytest
import re

type_id_regex = re.compile(r"^/[^/]*/[^/]*$")


@pytest.mark.slow
def test_insert(psql_service):  # noqa F811
    connection = get_connection(psql_service)
    with get_test_fh() as fh:
        i = 0
        for type_, id_, book in stream_objects(stream_file(fh, "bz2")):
            assert type_id_regex.match(type_)
            assert type_id_regex.match(id_)
            i += 1
        assert i == 1000

    print(connection)
