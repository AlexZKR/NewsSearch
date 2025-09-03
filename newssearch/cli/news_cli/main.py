from datetime import datetime
from typing import Annotated

import typer

from newssearch.cli.news_cli.utils import configure_logging, get_files_for_download
from newssearch.config import settings
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.transport.requests_transport import BaseHTTPTransport
from newssearch.tasks.news_etl.news_etl import NewsETL
from newssearch.tasks.news_etl.utils import format_year_month


def main(  # noqa: PLR0915
    year_month: Annotated[
        datetime, typer.Argument(formats=[settings.NEWS_ETL_SETTINGS.date_format])
    ],
):
    """Download a file from CC-NEWS dataset

    Args:
        year_month (str): The Year and Month in YYYY/MM format (e.g., 2023/09).
    """
    configure_logging()

    transport = BaseHTTPTransport()
    client = NewsClient(transport)
    ETL = NewsETL(client)
    typer.echo(
        f"This command lets you load WARC files from CC-NEWS dataset into ElasticSearch. \
        \n\nYou have entered year and month: {format_year_month(date=year_month)} \
        \n\nNow the app will query CC-NEWS to see, if any news are available for this date.\n"
    )
    paths_file = ETL.get_paths_file(year_month)
    if not paths_file:
        typer.echo("Try once again with another date!")
        typer.Exit(-1)
        return

    typer.echo(paths_file.table_output())
    typer.echo(
        "Now, you can choose range (or just 1) from ID column and those files will be loaded into Elasticsearch."
        "\n\nATTENTION: EACH FILE IS APPROX. 1 GB IN SIZE, SO DON'T SPECIFY MANY!\n"
    )

    typer.echo(f"Range of available IDs: {paths_file.id_range}")

    files = get_files_for_download(paths_file.filepaths)
    ETL.run(files)


if __name__ == "__main__":
    typer.run(main)
