"""
Functions to manage connection to the postgresql database.
"""
from collections import Counter
from enum import Enum
from itertools import groupby
import re
from oltools.parsers import stream_file
from oltools.parsers import stream_objects
from oltools.cli_utils import console, decimal
from pathlib import Path
from typing import Optional
import atexit
import io
import psycopg2
import sqlite3


def get_connection(url: str):
    if url.startswith("postgresql://"):
        return psycopg2.connect(url)
    elif url.startswith("sqlite://"):
        return sqlite3.connect(url.replace("sqlite://", ""), isolation_level="DEFERRED")


def empty_update_progress(**_):
    pass


def insert_from_file(
    file_path,
    url,
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
    connection = get_connection(url)
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
        if url.startswith("postgresql://"):
            insert_chunk = insert_chunk_postgresql
        elif url.startswith("sqlite://"):
            insert_chunk = insert_chunk_sqlite
        for chunk_lines in iterator_of_iterators(
            stream_objects(stream_file(fh, filetype)), chunk_size
        ):
            insert_chunk(cursor, chunk_lines, update_progress)
    connection.commit()
    connection.close()


samples = {}
COLLECT = 10


def save_samples():
    console.print("[blue]Exiting")
    with open("/tmp/openlibrary_samples.txt", "w") as samples_file:
        for sample in samples.values():
            lines = ["\t".join(el) for el in sample]
            samples_file.write("".join(lines))


atexit.register(save_samples)


def insert_chunk_sqlite(cursor, chunk_lines, update_progress):
    to_insert = list(chunk_lines)
    # Divide the cunk_lines by type
    to_insert.sort(key=lambda line: line[0])
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = OFF")
    for key, group_with_type in groupby(to_insert, lambda x: x[0]):
        group_with_type = tuple(group_with_type)
        current_size = len(samples.get(key, []))
        if current_size < COLLECT:
            to_add = COLLECT - current_size
            samples.setdefault(key, []).extend(group_with_type[:to_add])
            save_samples()
        group = remove_first_items(group_with_type)
        cursor.executemany(
            f'INSERT INTO "{key}" '
            "(key, revision, last_modified, data)"
            " VALUES (?, ?, ?, ?)",
            group,
        )
    cursor.connection.commit()
    update_progress(Counter(line[0] for line in to_insert))


def remove_first_items(iterable):
    for collection in iterable:
        yield collection[1:]


def insert_chunk_postgresql(cursor, chunk_lines, update_progress):
    lines = {}
    original_lines = {}
    for line in chunk_lines:
        if line[0] not in VALID_TYPES:
            continue
        original_lines.setdefault(line[0], []).append(line)
        lines.setdefault(line[0], []).append(get_psql_line(line))

    for type_, objs in lines.items():
        insert_chunk_postgresql_table(
            cursor, objs, original_lines[type_], update_progress, type_
        )


def insert_chunk_postgresql_table(
    cursor, lines, original_lines, update_progress, table_name
):
    try:
        cursor.copy_from(
            io.StringIO("".join(lines)),
            table_name,
            sep="|",
            null=r"\N",
            columns=("key", "revision", "last_modified", "data"),
        )
        progress_update = Counter(line[0] for line in original_lines)
    except psycopg2.errors.Error as error:
        cursor.connection.rollback()
        # First some error reporting
        try:
            error_line_number = int(
                re.search("COPY oldata, line ([0-9]+),", error.pgerror).groups()[0]
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
        progress_update = {}
        for i, line in enumerate(lines):
            if i == error_line_number - 1:
                continue
            try:
                cursor.copy_from(
                    io.StringIO(line),
                    table_name,
                    sep="|",
                    null=r"\N",
                    columns=("key", "revision", "last_modified", "data"),
                )
                update_progress({original_lines[i][0]: cursor.rowcount})
            except psycopg2.errors.Error as error:
                cursor.connection.rollback()
                console.print(f"[red]Error in line[/red] [blue]{line}[/blue]")
    cursor.connection.commit()
    update_progress(progress_update)


def get_psql_line(line):
    if len(line[1]) > 20:
        console.print(f"[red]Id too long: {line[1]}")
    result = "|".join((line[1][:20], line[2], line[3], clean_csv_value(line[4])))
    return result


def clean_csv_value(value: Optional[str]):
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


def create_oldata_tables(url):
    connection = get_connection(url)
    cursor = connection.cursor()
    for type_ in DataType:
        cursor.execute(
            f'CREATE TABLE IF NOT EXISTS "{type_.name}" ('
            "key char(13), "  # I observed ids are always 10 characters long
            "revision INT, "
            "last_modified TIMESTAMP, "
            "data JSONB"
            ");"
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


class DataType(Enum):
    """Different kind of records that can be found in the OL dump."""

    edition = "edition"
    work = "work"
    author = "author"
    delete = "delete"
    page = "page"
    redirect = "redirect"
    i18n = "i18n"
    property = "property"
    type = "type"
    collection = "collection"
    backreference = "backreference"
    permission = "permission"
    usergroup = "usergroup"
    macro = "macro"
    about = "about"
    template = "template"
    content = "content"
    doc = "doc"
    i18n_page = "i18n_page"
    user = "user"
    language = "language"
    volume = "volume"


VALID_TYPES = set(el.name for el in DataType)
