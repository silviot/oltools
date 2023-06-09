from pathlib import Path
from oltools.db import create_oldata_tables
import psycopg2
import pytest
import os
import sys
import tempfile


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
    create_oldata_tables(url)
    print("Table oldata created")
    yield url
    print("\nStopping db")


@pytest.mark.slow
@pytest.fixture(scope="session")
def sqlite_tmpfile():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    url = "sqlite:///" + path
    print(f"Using temporary sqlite file {path}")
    create_oldata_tables(url)
    yield url
    if Path(path).exists():
        Path(path).unlink()
    print(f"{path} deleted")


@pytest.fixture(scope="session", params=["postgresql", "sqlite"])
def db_service(request):
    if request.param == "postgresql":
        return request.getfixturevalue("psql_service")
    elif request.param == "sqlite":
        return request.getfixturevalue("sqlite_tmpfile")


def is_responsive(url):
    try:
        psycopg2.connect(url)
        return True
    except psycopg2.OperationalError:
        sys.stdout.write(".")
        return False
