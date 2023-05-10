from pathlib import Path
import pytest
import psycopg


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
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(url)
    )
    return url


def is_responsive(url):
    try:
        psycopg.connect(url)
        return True
    except psycopg.OperationalError:
        return False
