from oltools.tests.fixtures import docker_compose_file, psql_service  # noqa: F401
import psycopg


def test_insert(psql_service):
    connection = psycopg.connect(psql_service)
