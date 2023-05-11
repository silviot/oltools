from setuptools import setup

setup(
    name="oltools",
    version="0.1",
    description="Tools for Open Library dumps",
    url="http://github.com/silviot/oltools",
    author="Flying Circus",
    author_email="silvio@tomatis.email",
    license="MIT",
    packages=["oltools"],
    install_requires=["psycopg2", "rich-click"],
    extras_require={
        "test": ["pytest", "pytest-postgresql", "pytest-docker"],
    },
    entry_points={
        "console_scripts": [
            "ol-populate-pg = oltools.cli:populate_db",
        ],
    },
    zip_safe=False,
)
