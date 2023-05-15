import rich_click as click
from oltools.db import insert_from_file
from oltools.db import create_oldata_table
from oltools.cli_utils import step_progress


@click.command()
@click.argument("filename")
@click.argument(
    "database-URL",
    default="sqlite:////tmp/oldata.db",
)
@click.option(
    "--chunk-size",
    default=7000,
)
@click.option(
    "--offset",
    default=0,
)
def populate_db(filename, database_url, chunk_size, offset):
    create_oldata_table(database_url)
    totals = {"global": 0}
    tasks = {}

    def update_progress(updates):
        for category, advance in updates.items():
            totals["global"] += 1
            if category not in totals:
                totals[category] = 0
            totals[category] += 1
            if category not in tasks and totals[category] > 10:
                tasks[category] = progress.add_task(f"[green]{category}", total=None)
            if category in tasks:
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
            filename,
            database_url,
            update_progress,
            file_wrapper=progress.wrap_file,
            chunk_size=chunk_size,
            offset=offset,
        )
