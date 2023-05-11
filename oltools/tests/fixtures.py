from pathlib import Path
from oltools.db import create_oldata_table
import pytest
import psycopg2
import sys


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    path = Path(__file__).parent / "docker-compose.yml"
    assert path.exists(), "docker-compose.yml not found"
    assert path.is_file(), "docker-compose.yml is not a file"
    return str(path)


@pytest.mark.slow
@pytest.fixture(scope="session")
def psql_service(docker_ip, docker_services):
    """Ensure that the postgresql service is up and responsive."""

    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("postgresql", 5432)
    # Build a postgresql URL to the container
    url = "postgresql://postgres:postgres@%s:%d/oltools-test" % (docker_ip, port)
    print("\nStarting db", end="")
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(url)
    )
    print("\nDb started")
    create_oldata_table(url)
    print("Table oldata created")
    yield url
    print("\nStopping db")


def is_responsive(url):
    try:
        psycopg2.connect(url)
        return True
    except psycopg2.OperationalError:
        sys.stdout.write(".")
        return False
