import gzip
import bz2


def extract_json(textline):
    """We need to get the JSON content of this line.
    We know the line has a prefix. We assume the character `{` does not appear
    in the prefix.
    """
    # We first find the index of the first `{` character.
    index = textline.find("{")
    return textline[index:]


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
