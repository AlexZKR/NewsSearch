from datetime import date, datetime

from newssearch.config import settings


def format_year_month(date: datetime | date) -> str:
    if isinstance(date, datetime):
        return date.date().strftime(settings.NEWS_ETL_SETTINGS.date_format)
    return date.strftime(settings.NEWS_ETL_SETTINGS.date_format)
