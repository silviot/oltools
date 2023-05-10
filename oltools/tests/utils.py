from contextlib import contextmanager
from pathlib import Path


TEST_FILE_PATH = Path(__file__).parent / "editions.txt.bz2"


@contextmanager
def get_test_fh():
    with open(TEST_FILE_PATH, "rb") as fh:
        yield fh


AUTHORS_TEXT = """
/type/author    /authors/OL32329A       1       2008-04-01T03:28:50.625462      {"name": "Peano, Giuseppe", "personal_name": "Peano, Giuseppe", "death_date": "1932.", "last_modified": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "key": "/authors/OL32329A", "birth_date": "1858", "type": {"key": "/type/author"}, "revision": 1}
/type/edition   /books/OL49233M 1       2008-04-01T03:28:50.625462      {"subtitle": "according to the Ausdehnungslehre of H. Grassmann", "lc_classifications": ["QA205 .P4313 2000"], "title": "Geometric calculus", "languages": [{"key": "/languages/eng"}], "subjects": ["Ausdehnungslehre.", "Logic, Symbolic and mathematical."], "publish_country": "mau", "by_statement": "Giuseppe Peano ; translated by Lloyd C. Kannenberg.", "type": {"key": "/type/edition"}, "revision": 1, "publishers": ["Birkha\u0308user"], "last_modified": {"type": "/type/datetime", "value": "2008-04-01T03:28:50.625462"}, "key": "/books/OL49233M", "authors": [{"key": "/authors/OL32329A"}], "publish_places": ["Boston"], "pagination": "xv, 150 p. :", "dewey_decimal_class": ["512/.5"], "notes": {"type": "/type/text", "value": "Includes bibliographical references and index."}, "number_of_pages": 150, "lccn": ["99051963"], "isbn_10": ["0817641262", "3764341262"], "publish_date": "2000", "work_title": ["Calcolo geometrico secondo l'Ausdehnungslehre di H. Grassmann."]}
/type/work      /works/OL15255036W      1       2010-07-23T08:50:12.958293      {"title": "Selected works of Giuseppe Peano translated [from the Italian]", "created": {"type": "/type/datetime", "value": "2010-07-23T08:50:12.958293"}, "last_modified": {"type": "/type/datetime", "value": "2010-07-23T08:50:12.958293"}, "subject_people": ["Giuseppe Peano (1858-1932)"], "key": "/works/OL15255036W", "authors": [{"type": {"key": "/type/author_role"}, "author": {"key": "/authors/OL32329A"}}], "latest_revision": 1, "type": {"key": "/type/work"}, "subjects": ["Mathematics", "Bibliography"], "revision": 1}
"""  # noqa: E501

AUTHORS = [
    {
        "name": "Peano, Giuseppe",
        "personal_name": "Peano, Giuseppe",
        "death_date": "1932.",
        "last_modified": {
            "type": "/type/datetime",
            "value": "2008-04-01T03:28:50.625462",
        },
        "key": "/authors/OL32329A",
        "birth_date": "1858",
        "type": {"key": "/type/author"},
        "revision": 1,
    },
    {
        "subtitle": "according to the Ausdehnungslehre of H. Grassmann",
        "lc_classifications": ["QA205 .P4313 2000"],
        "title": "Geometric calculus",
        "languages": [{"key": "/languages/eng"}],
        "subjects": ["Ausdehnungslehre.", "Logic, Symbolic and mathematical."],
        "publish_country": "mau",
        "by_statement": "Giuseppe Peano ; translated by Lloyd C. Kannenberg.",
        "type": {"key": "/type/edition"},
        "revision": 1,
        "publishers": ["Birkha\u0308user"],
        "last_modified": {
            "type": "/type/datetime",
            "value": "2008-04-01T03:28:50.625462",
        },
        "key": "/books/OL49233M",
        "authors": [{"key": "/authors/OL32329A"}],
        "publish_places": ["Boston"],
        "pagination": "xv, 150 p. :",
        "dewey_decimal_class": ["512/.5"],
        "notes": {
            "type": "/type/text",
            "value": "Includes bibliographical references and index.",
        },
        "number_of_pages": 150,
        "lccn": ["99051963"],
        "isbn_10": ["0817641262", "3764341262"],
        "publish_date": "2000",
        "work_title": [
            "Calcolo geometrico secondo l'Ausdehnungslehre di H. Grassmann."
        ],
    },
    {
        "title": "Selected works of Giuseppe Peano translated [from the Italian]",
        "created": {"type": "/type/datetime", "value": "2010-07-23T08:50:12.958293"},
        "last_modified": {
            "type": "/type/datetime",
            "value": "2010-07-23T08:50:12.958293",
        },
        "subject_people": ["Giuseppe Peano (1858-1932)"],
        "key": "/works/OL15255036W",
        "authors": [
            {
                "type": {"key": "/type/author_role"},
                "author": {"key": "/authors/OL32329A"},
            }
        ],
        "latest_revision": 1,
        "type": {"key": "/type/work"},
        "subjects": ["Mathematics", "Bibliography"],
        "revision": 1,
    },
]
