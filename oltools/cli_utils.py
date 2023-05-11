import rich_click as click
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    ProgressColumn,
    Text,
)


click.rich_click.USE_RICH_MARKUP = True


class ThingsSpeedColumn(ProgressColumn):
    """Renders transfer speed of things/second.
    Adapted from https://github.com/Textualize/rich/blob/v13.3.5/rich/progress.py#L903-L912
    """

    def render(self, task):
        """Show data transfer speed."""
        speed = task.finished_speed or task.speed
        decimal_kwargs = {}
        if "thing_name" in task.fields:
            decimal_kwargs["units"] = (
                "thousands",
                "millions",
                "billions",
                "trillions",
                "PB",
                "EB",
                "ZB",
                "YB",
            )
        if speed is None:
            return Text("?", style="progress.data.speed")
        data_speed = decimal(int(speed), **decimal_kwargs)
        return Text(f"{data_speed}/s", style="progress.data.speed")


step_progress = Progress(
    TextColumn("  "),
    TimeElapsedColumn(),
    BarColumn(),
    TextColumn(
        "[bold purple]{task.completed} {task.description} {task.percentage:.0f}%"
    ),
    ThingsSpeedColumn(),
    SpinnerColumn("simpleDots"),
)


def _to_str(
    size,
    suffixes,
    base,
    *,
    precision=1,
    separator=" ",
):
    if size == 1:
        return "1 row"
    elif size < base:
        return "{:,} rows".format(size)

    for i, suffix in enumerate(suffixes, 2):  # noqa: B007
        unit = base**i
        if size < unit:
            break
    return "{:,.{precision}f}{separator}{}".format(
        (base * size / unit),
        suffix,
        precision=precision,
        separator=separator,
    )


def decimal(
    size: int,
    *,
    precision=1,
    separator=" ",
    units=("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"),
):
    """Copied from https://github.com/Textualize/rich/blob/v13.3.5/rich/filesize.py#L53-L89
    Convert a filesize in to a string (powers of 1000, SI prefixes).

    In this convention, ``1000 B = 1 kB``.

    This is typically the format used to advertise the storage
    capacity of USB flash drives and the like (*256 MB* meaning
    actually a storage capacity of more than *256 000 000 B*),
    or used by **Mac OS X** since v10.6 to report file sizes.

    Arguments:
        int (size): A file size.
        int (precision): The number of decimal places to include (default = 1).
        str (separator): The string to separate the value from the units.
        tuple (units): The units to use.

    Returns:
        `str`: A string containing a abbreviated file size and units.

    Example:
        >>> filesize.decimal(30000)
        '30.0 kB'
        >>> filesize.decimal(30000, precision=2, separator="")
        '30.00kB'

    """
    return _to_str(
        size,
        units,
        1000,
        precision=precision,
        separator=separator,
    )
