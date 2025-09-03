from logging import getLogger
from typing import Any
from urllib import parse

from pydantic import BaseModel, Field, computed_field

from newssearch.config import settings
from newssearch.infrastructure.clients.news.enums import CCDataSet

logger = getLogger(__name__)

WARC_EXT_SUFFIX = ".warc.gz"


class WARCInputSchema(BaseModel):
    pass


class WarcFileSchema(BaseModel):
    """WARC filename, parsed from WarcPathsFile

    Example of one filename:
    'crawl-data/CC-NEWS/2025/01/CC-NEWS-20250101020153-00156.warc.gz'
    """

    filepath: str = Field(..., description="Original filepath")

    dataset: CCDataSet
    year: str
    month: str
    timestamp: str
    id: str

    @classmethod
    def parse_warc_path(cls, filepath: str):
        """Parse the filename and init schema fields"""
        data: dict[str, Any] = {}
        try:
            parts = filepath.removesuffix(WARC_EXT_SUFFIX).split("/")

            # metadata
            data["filepath"] = filepath
            data["dataset"] = parts[1]
            data["year"] = parts[2]
            data["month"] = parts[3]

            # file identificators
            ids = parts[4].split("-")
            data["timestamp"] = ids[2]
            data["id"] = ids[3]

        except IndexError as exc:
            logger.warning(f"Invalid WARC filepath: {filepath}. Error: {exc}")
            return None
        return cls(**data)

    def __str__(self) -> str:
        return f"{self.year}/{self.month} - {self.timestamp} - {self.id}"


class WarcPathsFile(BaseModel):
    """WARC file listings by month

    Example of URL:
    https://data.commoncrawl.org/crawl-data/CC-NEWS/2024/01/warc.paths.gz

    """

    url: str = Field(..., description="URL of paths file")

    dataset: CCDataSet
    year: str
    month: str
    filepaths: list[WarcFileSchema] = Field(default_factory=list)

    @classmethod
    def parse_warc_paths_url(cls, url: str):
        """Parse the URL and init schema fields"""
        data: dict[str, Any] = {}
        url_path = parse.urlsplit(url=url).path.split("/")  # remove whitespace

        # metadata
        data["url"] = url
        data["dataset"] = url_path[2]
        data["year"] = url_path[3]
        data["month"] = url_path[4]

        return cls(**data)

    def parse_warc_filepaths(self, filepaths: list[str]):
        """Fill filepaths field, filtering out invalid paths"""
        parsed_paths = []
        for filepath in filepaths:
            parsed = WarcFileSchema.parse_warc_path(filepath)
            if parsed is not None:
                parsed_paths.append(parsed)

        self.filepaths = parsed_paths

    @computed_field  # type: ignore [prop-decorator]
    @property
    def id_range(self) -> str:  # pragma: nocover
        """Get first and last ID's of WARC files in this paths file."""
        first = f"{self.filepaths[0].id}"
        last = ""
        if self.filepaths[-1].id != self.filepaths[0].id:
            last = f" - {self.filepaths[-1].id}"
        return first + last if last else first

    def table_output(self) -> str:  # pragma: nocover
        if not self.filepaths:
            return "No WARC filepaths."
        table_len = settings.NEWS_ETL_SETTINGS.warc_paths_table_length

        rows = [
            (f"{p.year}/{p.month}", p.timestamp, p.id)
            for p in self.filepaths[:table_len]
        ]

        header = ("Date", "Timestamp", "ID")
        w0, w1, w2 = (max(len(x) for x in col) for col in zip(*([header] + rows)))
        fmt = f"{{:<{w0}}}  {{:<{w1}}}  {{:>{w2}}}"

        lines = [fmt.format(*header)] + [fmt.format(*r) for r in rows]

        len_title = (
            f"First {table_len}"
            if len(self.filepaths) > table_len
            else f"First {len(self.filepaths)}"
        )

        result = f"{len_title} WARC filepaths:\n" + "\n".join(lines)
        if len(self.filepaths) > table_len:
            result += f"\n...{len(self.filepaths) - table_len} more"
        return result
