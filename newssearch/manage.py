import logging
from datetime import datetime
from logging import getLogger
from typing import Annotated

import typer
import uvicorn

from newssearch.config import settings

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
def get_news(year_month: Annotated[datetime, typer.Argument(formats=["%Y/%m"])]):
    """Download a file from CC-NEWS dataset

    Args:
        year_month (str): The Year and Month in YYYY/MM format (e.g., 2023/09).
    """
    logger.info(f"Selected date: {year_month.date()}")


@app.callback()
def callback():
    pass


if __name__ == "__main__":
    app()
