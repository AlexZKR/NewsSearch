import logging
import re
from datetime import datetime
from logging import getLogger
from typing import Annotated

import typer
import uvicorn

from newssearch.config import settings
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport
from newssearch.tasks.news_etl.news_etl import NewsETL
from newssearch.tasks.news_etl.utils import format_year_month

app = typer.Typer()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = getLogger(__name__)


@app.command()
def runserver():
    uvicorn.run(**settings.UVICORN_SETTINGS.model_dump())


@app.command("get-news")
def get_news(  # noqa: PLR0915
    year_month: Annotated[
        datetime, typer.Argument(formats=[settings.NEWS_ETL_SETTINGS.date_format])
    ],
):
    """Download a file from CC-NEWS dataset

    Args:
        year_month (str): The Year and Month in YYYY/MM format (e.g., 2023/09).
    """
    transport = BaseHTTPTransport()
    client = NewsClient(transport)
    ETL = NewsETL(client)
    print(
        f"This command lets you load WARC files from CC-NEWS dataset into ElasticSearch. \
        \n\nYou have entered year and month: {format_year_month(date=year_month)} \
        \n\nNow the app will query CC-NEWS to see, if any news are available for this date."
    )
    paths_file = ETL.get_paths_file(year_month)
    if not paths_file:
        print("Try once again with another date!")
        return

    print("\n")
    print(paths_file.table_output())
    print(
        "Now, you can choose range (or just 1) from ID column and those files will be loaded into Elasticsearch."
        "\n\nATTENTION: EACH FILE IS APPROX. 1 GB IN SIZE, SO DON'T SPECIFY MANY!\n"
    )

    print(f"Range of available IDs: {paths_file.id_range}")

    # --- helper to parse and validate the user's input ---
    def parse_id_range(raw: str, available_ids: set[str]) -> tuple[str, str]:
        raw = raw.strip()
        if not raw:
            raise ValueError("empty input")

        # allow hyphen variants with optional spaces
        parts = re.split(r"\s*[-–—]\s*", raw)
        if len(parts) == 1:
            start_s = end_s = parts[0]
        elif len(parts) == 2:  # noqa: PLR2004
            start_s, end_s = parts
            if not start_s:
                raise ValueError("range must have a start id")
            if not end_s:
                # "03802-" treat as single id (same as start)
                end_s = start_s
        else:
            raise ValueError("too many separators")

        if not (start_s.isdigit() and end_s.isdigit()):
            raise ValueError("ids must be numeric")

        # numeric validation using available ids
        width = max(len(i) for i in available_ids)  # preserve zero-padding
        available_ints = sorted(int(i) for i in available_ids)
        min_id, max_id = available_ints[0], available_ints[-1]

        start_i, end_i = int(start_s), int(end_s)
        if start_i > end_i:
            raise ValueError("start id must be <= end id")
        if not (min_id <= start_i <= max_id and min_id <= end_i <= max_id):
            raise ValueError(
                f"ids out of available range: {min_id:0{width}}-{max_id:0{width}}"
            )

        # return zero-padded strings matching existing ids format
        return str(start_i).zfill(width), str(end_i).zfill(width)

    available_ids = {p.id for p in paths_file.filepaths}  # set of strings like "03802"

    # --- get input (either from option or interactively) ---

    # interactive loop until valid or user quits
    while True:
        try:
            raw = typer.prompt(
                "Enter ID or range (e.g. 03802 or 03802-03807). 'q' to quit"
            )
        except (EOFError, KeyboardInterrupt):
            typer.echo("Aborted.")
            raise typer.Exit(code=1)

        if raw.strip().lower() in {"q", "quit", "exit"}:
            typer.echo("Aborted.")
            raise typer.Exit(code=0)

        try:
            start_id, end_id = parse_id_range(raw, available_ids)
            break
        except ValueError as exc:
            typer.echo(f"Invalid input: {exc}. Please try again.")

    if not end_id or start_id == end_id:
        typer.echo(f"Loading file {start_id} ...")
    else:
        typer.echo(f"Loading files from {start_id} to {end_id} ...")


@app.callback()
def callback():
    pass


if __name__ == "__main__":
    app()
