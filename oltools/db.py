"""
Functions to manage connection to the postgresql database.
"""
from oltools.parsers import stream_file
from oltools.parsers import stream_objects
from pathlib import Path
import io
import psycopg


def get_connection(url):
    return psycopg.connect(url)


def empty_update_progress(**_):
    pass


def insert_from_file(
    file_path,
    psql_service,
    update_progress=empty_update_progress,
    file_wrapper=None,
    chunk_size=100,
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
        if file_wrapper:
            fh = file_wrapper(
                fh,
                total=file_path.stat().st_size,
                description=f"Reading from {file_path.name}",
            )
        for chunk_lines in iterator_of_iterators(
            stream_objects(stream_file(fh, filetype)), chunk_size
        ):
            for type_, id_, obj in chunk_lines:
                cursor.execute(
                    "INSERT INTO oldata (type_id, id, data) VALUES (%s, %s, %s);",
                    (type_, id_, obj),
                )
                update_progress(category=type_.split("/")[-1], advance=1)
    connection.commit()
    connection.close()


def iterator_of_iterators(baseiter, chunksize):
    """Yield successive chunks from baseiter."""
    emitted_info = {}
    emitted_info["total"] = 0
    emitted_info["finished"] = False
    iterator = iter(baseiter)

    def to_return():
        current = None
        try:
            while True:
                current = next(iterator)
                emitted_info["total"] += 1
                yield current
                if emitted_info["total"] == chunksize:
                    emitted_info["total"] = 0
                    break
        except StopIteration:
            emitted_info["finished"] = True

    while not emitted_info["finished"]:
        yield to_return()


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


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter):
        self._iter = iter
        self._buff = ""

    def readable(self) -> bool:
        return True

    def _read1(self, n=None):
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n=None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)
