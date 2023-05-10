"""
Functions to manage connection to the postgresql database.
"""
import psycopg


def get_connection(url):
    return psycopg.connect(url)


def setup_db(connection):
    import pdb

    pdb.set_trace()
