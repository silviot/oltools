import rich_click as click
from oltools.db import insert_from_file
from oltools.db import create_oldata_table
from oltools.cli_utils import step_progress


@click.command()
@click.argument("filename")
@click.argument(
    "postgres-URL",
    default="postgresql://openlibrary:openlibrary@localhost:5432/openlibrary",
)
def populate_db(filename, postgres_url):
    create_oldata_table(postgres_url)
    totals = {"global": 0}
    tasks = {}

    def update_progress(category, advance):
        totals["global"] += 1
        category_name = category.split("/")[-1]
        if category not in tasks:
            totals[category] = 0
            tasks[category] = progress.add_task(f"[green]{category_name}", total=None)
        totals[category] += 1
        progress.update(
            tasks[category],
            advance=advance,
        )
        progress.update(
            tasks["global"],
            advance=advance,
        )

    with step_progress as progress:
        tasks["global"] = progress.add_task(
            "[red]Populating db", total=None, thing_name="entities"
        )
        insert_from_file(
            filename, postgres_url, update_progress, file_wrapper=progress.wrap_file
        )
