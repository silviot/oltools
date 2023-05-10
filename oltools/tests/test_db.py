from oltools.tests.fixtures import docker_compose_file as _, psql_service as _  # noqa
import psycopg
import pytest


@pytest.mark.slow
def test_insert(psql_service):
    connection = psycopg.connect(psql_service)
    print(connection)
