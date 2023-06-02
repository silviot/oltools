"""
Provide info about the structure of the database.
"""
from enum import Enum
import json


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
    volume = "volume"
    user = "user"
    i18n_page = "i18n_page"
    language = "language"
    home = "home"
    rawtext = "rawtext"
    object = "object"
    subject = "subject"
    library = "library"
    scan_record = "scan_record"
    uri = "uri"
    series = "series"
    scan_location = "scan_location"
    place = "place"
    local_id = "local_id"
    list = "list"


VALID_TYPES = set(el.name for el in DataType)

###################################################

# What follows was written by ChatGPT using this prompt:

_prompt = """
I have an sqlite database where all info is stored as JSON in a "data" column.

I'd like to use python to extract the content from the  JSON and store it into
properly typed columns. You will output python code. Two variable definitions,
defining a variable with a "CREATE TABLE" statement, one with an "INSERT"
statement, and a python function. The variables should be named
`create_${TABLE_NAME}` and `insert_${TABLE_NAME}`. Try to keep the code short,
but readable.

The SQL statement should create the table to hold the data. It should have the
same name as the given table. Make sure you use the correct type for timestamps.

Write a python function that takes a JSON string in the format of the JSON
provided and returns a list of values to insert into the table. The function
name should be ${TABLE_NAME}_to_values. No need to convert to datetime in the
python function: timestamps can stay as strings. They need to be defined as
timestamps only in the table definition. Encode list of values as comma
separated.

Write the code first, and any comments afterwards. Assume `json` is already
imported.

Here's the data:

"""

create_work = """
CREATE TABLE work (
    key TEXT,
    latest_revision INTEGER,
    last_modified TIMESTAMP,
    created TIMESTAMP,
    title TEXT,
    authors TEXT,
    subjects TEXT,
    subject_places TEXT,
    covers TEXT,
    type TEXT,
    revision INTEGER
);
"""

insert_work = """
INSERT INTO work (
    key,
    latest_revision,
    last_modified,
    created,
    title,
    authors,
    subjects,
    subject_places,
    covers,
    type,
    revision
) VALUES (?,?,?,?,?,?,?,?,?,?,?);
"""


def work_to_values(json_str):
    data = json.loads(json_str)

    # Extract data from JSON
    key = data.get("key", "")
    latest_revision = data.get("latest_revision", "")
    last_modified = data.get("last_modified", {}).get("value", "")
    created = data.get("created", {}).get("value", "")
    title = data.get("title", "")

    authors = ", ".join(
        [author.get("author", {}).get("key", "") for author in data.get("authors", [])]
    )

    subjects = ", ".join(data.get("subjects", []))
    subject_places = ", ".join(data.get("subject_places", []))

    covers = ", ".join([str(cover) for cover in data.get("covers", [])])

    type_key = data.get("type", {}).get("key", "")
    revision = data.get("revision", "")

    # Return data as a list
    return [
        key,
        latest_revision,
        last_modified,
        created,
        title,
        authors,
        subjects,
        subject_places,
        covers,
        type_key,
        revision,
    ]
