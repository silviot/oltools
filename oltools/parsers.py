from oltools.cli_utils import console
import bz2
import gzip


def stream_file(fh, compression="gz"):
    """Receives a file handle of a file, possibly compressed with gzip (`gz`),
    bzip2 (`bz2`), or uncompressed (`None`).
    Returns a generator that yields the lines of the file.
    """
    if compression == "gz":
        # We create a gzip file handle from the file handle.
        fh = gzip.GzipFile(fileobj=fh)
    elif compression == "bz2":
        # We create a bzip2 file handle from the file handle.
        fh = bz2.open(fh)
    # We iterate through the lines of the file handle.
    for line in fh:
        # We decode the line and yield it.
        yield line.decode("utf-8")


def stream_objects(lines):
    """Takes lines in the format:
    TYPE ID <OTHER THINGS THAT WILL BE IGNORED> JSON
    It isolates the JSON and yields the tuple (TYPE, ID, OBJ).
    """
    for line in lines:
        try:
            type_, key, revision, last_modified, obj = line.split("\t", 4)
            yield type_, key, revision, last_modified, obj
        except ValueError:
            console.print("[red]Error parsing line[/red]:")
            console.print(line)
