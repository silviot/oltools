from pathlib import Path
import pytest
import psycopg
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
    connection = psycopg.connect(url)
    # Create table oldata
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE oldata (type_id VARCHAR(100), id VARCHAR(100), data JSONB);"
    )
    connection.commit()
    connection.close()
    print("Table oldata created")
    yield url
    print("\nStopping db")


def is_responsive(url):
    try:
        psycopg.connect(url)
        return True
    except psycopg.OperationalError:
        sys.stdout.write(".")
        return False
