import rich_click as click
from oltools.db import insert_from_file
from oltools.db import create_oldata_table
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)


click.rich_click.USE_RICH_MARKUP = True

step_progress = Progress(
    TextColumn("  "),
    TimeElapsedColumn(),
    BarColumn(),
    TextColumn("[bold purple]{task.completed} {task.description}"),
    SpinnerColumn("simpleDots"),
)


@click.command()
@click.argument("filename")
# @click.argument("postgres URL"="postgresql://openlibrary:openlibrary@127.0.0.1:5432/openlibrary")
# Ok, the above is wrong. Here's the right version with default value:
@click.argument(
    "postgres-URL",
    default="postgresql://openlibrary:openlibrary@localhost:5432/openlibrary",
)
def populate_db(filename, postgres_url):
    create_oldata_table(postgres_url)
    done = {"done": 0}

    def update_progress(advance):
        done["done"] += 1
        progress.update(
            task1, advance=advance, description=f"Populating db with {done}"
        )

    with step_progress as progress:
        task1 = progress.add_task("[red]Populating db", total=None)
        insert_from_file(filename, postgres_url, update_progress)
