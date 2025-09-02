import logging
from datetime import datetime
from logging import getLogger
from typing import Annotated

import typer
import uvicorn

from newssearch.config import settings
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport
from newssearch.tasks.news_etl.news_etl import NewsETL
from newssearch.tasks.news_etl.utils import format_year_month, parse_id_range

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

    files = get_files_for_download(paths_file.filepaths)
    ETL.run(files)


@app.callback()
def callback():
    pass


def get_files_for_download(
    files: list[WarcPathSchema],
) -> list[WarcPathSchema]:
    id_to_file_map = {p.id: p for p in files}
    available_ids = set(id_to_file_map.keys())

    while True:
        raw = process_user_input()
        try:
            start_id, end_id = parse_id_range(raw, available_ids)

            # Validate that both IDs exist
            if start_id not in available_ids:
                raise ValueError(f"Start ID {start_id} not found in available files")
            if end_id not in available_ids:
                raise ValueError(f"End ID {end_id} not found in available files")

            break
        except ValueError as exc:
            typer.echo(f"Invalid input: {exc}. Please try again.")

    start_file = id_to_file_map[start_id]
    end_file = id_to_file_map[end_id]

    start_ind = files.index(start_file)
    end_ind = files.index(end_file)

    # Ensure start index comes before end index
    if start_ind > end_ind:
        start_ind, end_ind = end_ind, start_ind
        start_id, end_id = end_id, start_id

    file_count = end_ind - start_ind + 1

    if file_count > 1:
        typer.echo(f"Loading {file_count} files from {start_id} to {end_id}...")
    else:
        typer.echo(f"Loading file {start_id}...")

    # Return the slice (inclusive of end index)
    return files[start_ind : end_ind + 1]


def process_user_input() -> str:
    try:
        raw = typer.prompt(
            "Enter ID or range (e.g. 03802 or 03802-03807), end inclusive. 'q' to quit"
        )
    except (EOFError, KeyboardInterrupt):
        typer.echo("Aborted.")
        raise typer.Exit(code=1)

    if raw.strip().lower() in {"q", "quit", "exit"}:
        typer.echo("Aborted.")
        raise typer.Exit(code=0)
    return raw


if __name__ == "__main__":
    app()
