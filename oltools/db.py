"""
Functions to manage connection to the postgresql database.
"""
from oltools.parsers import stream_file
from oltools.parsers import stream_objects
from pathlib import Path
import psycopg


def get_connection(url):
    return psycopg.connect(url)


def insert_from_file(
    file_path, psql_service, update_progress=lambda advance=None: advance
):
    filetype = None
    file_path = Path(file_path)
    if file_path.suffix == ".bz2":
        filetype = "bz2"
    elif file_path.suffix == ".gz":
        filetype = "gz"
    connection = get_connection(psql_service)
    cursor = connection.cursor()
    with open(file_path, "rb") as fh:
        for type_, id_, obj in stream_objects(stream_file(fh, filetype)):
            update_progress(advance=1)
            cursor.execute(
                "INSERT INTO oldata (type_id, id, data) VALUES (%s, %s, %s);",
                (type_, id_, obj),
            )
    connection.commit()
    connection.close()


def create_oldata_table(url):
    connection = psycopg.connect(url)
    # Create table oldata
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS oldata (type_id VARCHAR(100), id"
        " VARCHAR(100), data JSONB);"
    )
    connection.commit()
    connection.close()
