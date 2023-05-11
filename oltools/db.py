"""
Functions to manage connection to the postgresql database.
"""
import re
from oltools.parsers import stream_file
from oltools.parsers import stream_objects
from oltools.cli_utils import console, decimal
from pathlib import Path
import io
import psycopg2


def get_connection(url):
    return psycopg2.connect(url)


def empty_update_progress(**_):
    pass


def insert_from_file(
    file_path,
    psql_service,
    update_progress=empty_update_progress,
    file_wrapper=None,
    chunk_size=100,
    offset=0,
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
        file_size = file_path.stat().st_size
        if offset:
            fh.seek(offset)
            file_size -= offset
        # XXX TODO It would be better to start from offset, instead of
        # pretending the file was smaller. The current API of wrap_file
        # doesn't allow to specify a starting figure.
        if file_wrapper:
            fh = file_wrapper(
                fh,
                total=file_size,
                description=f"Reading from {file_path.name}",
            )
        console.print(
            f"[blue]Start processing file {file_path.name} "
            f"[/blue]([red]{decimal(file_size)}[/red])"
        )
        for chunk_lines in iterator_of_iterators(
            stream_objects(stream_file(fh, filetype)), chunk_size
        ):
            lines = []
            for line in chunk_lines:
                lines.append(get_line(line))
            try:
                cursor.copy_from(
                    io.StringIO("".join(lines)),
                    "oldata",
                    sep="|",
                    null=r"\N",
                    columns=("type_id", "id", "data"),
                )
                howmany = cursor.rowcount
            except psycopg2.errors.InvalidTextRepresentation as error:
                connection.rollback()
                try:
                    error_line_number = int(
                        re.search(
                            "COPY oldata, line ([0-9]+),", error.pgerror
                        ).groups()[0]
                    )
                    console.print(
                        "[red]Error in line[/red] [blue]"
                        + str(error_line_number)
                        + f"[/blue]: {error.pgerror}"
                    )
                    console.print(lines[error_line_number - 1])
                except (IndexError, AttributeError):
                    console.print(error.pgerror)
                # Done with error reporting. Now let's try again,
                # this time in a less efficient fashion, one line at a time.
                # We could exclude the faulty line, but who knows if there was
                # another one after it.
                howmany = 0
                for i, line in enumerate(lines):
                    if i == error_line_number - 1:
                        continue
                    try:
                        cursor.copy_from(
                            io.StringIO(line),
                            "oldata",
                            sep="|",
                            null=r"\N",
                            columns=("type_id", "id", "data"),
                        )
                        howmany += cursor.rowcount
                    except psycopg2.errors.InvalidTextRepresentation as error:
                        connection.rollback()
                        console.print(f"[red]Error in line[/red] [blue]{line}[/blue]")
            connection.commit()
            update_progress(category="generic", advance=howmany)
    connection.commit()
    connection.close()


def get_line(line):
    if len(line[0]) > 100:
        console.print(f"[red]Type too long: {line[0]}")
    if len(line[1]) > 100:
        console.print(f"[red]Id too long: {line[1]}")
    result = "|".join((line[0][:100], line[1][:100], clean_csv_value(line[2])))
    return result


def clean_csv_value(value):
    if value is None:
        return r"\N"
    return str(value).replace("\\", "\\\\").replace("|", r"\|")


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
    connection = get_connection(url)
    # Create table oldata
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS oldata "
        "(type_id VARCHAR(100), "
        "id VARCHAR(100), "
        "data JSONB);"
    )
    connection.commit()
    connection.close()


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter):
        self._iter = iter
        self._buff = ""
        self.results = []

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
        result = "".join(line)
        self.results.append(result)
        return result
