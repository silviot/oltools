from oltools.parsers import stream_objects
from oltools.parsers import stream_file
from oltools.tests.utils import get_test_fh
from oltools.tests.utils import AUTHORS
from oltools.tests.utils import AUTHORS_TEXT
import json


def test_author():
    for i, line in enumerate(AUTHORS_TEXT.strip().splitlines()):
        author = line.split("\t", 4)[-1]
        assert json.loads(author) == AUTHORS[i]


def test_stream_file():
    # Open the file in raw mode (no decoding)

    with get_test_fh() as fh:
        i = 0
        for line in stream_file(fh, "bz2"):
            i += 1
        assert i == 1000


def test_stream_objects():
    i = 0
    with get_test_fh() as fh:
        for type_, key, revision, last_modified, book_json in stream_objects(
            stream_file(fh, "bz2")
        ):
            assert type_.count("/") == 0, "There should be no slashes in the type"
            assert key.count("/") == 0, "There should be no slashes in the key"
            assert int(revision) > 0
            book = json.loads(book_json)
            assert book["title"]
            i += 1
    assert i == 1000
