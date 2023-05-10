from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa
import psycopg
import pytest


@pytest.mark.slow
def test_insert(psql_service):  # noqa F811
    connection = psycopg.connect(psql_service)
    print(connection)
