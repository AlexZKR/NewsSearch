import concurrent
import concurrent.futures
import json
import os
import tempfile
from collections.abc import Iterator
from datetime import date
from logging import getLogger
from urllib.parse import urlparse

import trafilatura
from warcio.archiveiterator import ArchiveIterator

from newssearch.config.settings import NewsETLSettings
from newssearch.infrastructure.clients.news.exceptions import FileNotFound
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcPathSchema, WarcPathsFile
from newssearch.tasks.news_etl.schemas import RecordContentSchema, WARCRecordSchema
from newssearch.tasks.news_etl.utils import format_year_month

logger = getLogger(__name__)


class NewsETL:
    def __init__(
        self, news_client: NewsClient, settings: NewsETLSettings = NewsETLSettings()
    ) -> None:
        self.client = news_client
        self.__paths_files: dict[str, WarcPathsFile] = {}
        self.settings = settings

    def run(self, files: list[WarcPathSchema]):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.settings.max_workers
        ) as executor:
            future_to_file = {executor.submit(self.process_file, f): f for f in files}

        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            try:
                future.result()
                print(f"Successfully processed {file.id}")
            except Exception as e:
                print(f"Error processing {file.id}: {e}")

    def process_file(self, file: WarcPathSchema):
        records = self.parse_warc(self.client.download_warc(file), file)
        print(records[0])
        print(records[1])
        print(len(records))

    def parse_warc(self, content_iterator: Iterator[bytes], file: WarcPathSchema):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for chunk in content_iterator:
                temp_file.write(chunk)
            temp_file_path = temp_file.name

        try:
            records = []
            with open(temp_file_path, "rb") as f:
                for record in ArchiveIterator(f):
                    if (
                        record.rec_type == "response"
                        and record.http_headers.get_header("Content-Type")
                        == "text/html"
                    ):
                        schema = WARCRecordSchema(
                            id=record.rec_headers.get_header("WARC-Record-ID"),
                            url=self.extract_top_level_domain(
                                record.rec_headers.get_header("WARC-Target-URI")
                            ),
                            date=record.rec_headers.get_header("WARC-Date"),
                            content_length=record.rec_headers.get_header(
                                "Content-Length"
                            ),
                            mime_type=record.http_headers.get_header("Content-Type")
                            if record.http_headers
                            else None,
                        )
                        content = self.extract_text_content(
                            record.content_stream().read(), schema.id
                        )
                        schema.content = content
                        records.append(schema)
            return records
        finally:
            os.unlink(temp_file_path)

    def get_paths_file(self, year_month: date) -> WarcPathsFile | None:
        ym = format_year_month(year_month)
        if file := self.__paths_files.get(ym):
            return file
        try:
            file = self.client.get_paths_file(year_month)
            self.__paths_files[ym] = file
            return file
        except FileNotFound:
            logger.warning(f"Paths file for {ym} not found!")
            return None

    def extract_top_level_domain(self, url: str):
        """Extract the top-level domain (TLD) from a URL."""
        try:
            parsed_url = urlparse(url)
            domain_parts = parsed_url.netloc.split(".")
            if len(domain_parts) > 1:
                return "." + domain_parts[-1]
            return domain_parts[0]
        except Exception as e:
            logger.warning(f"Error extracting TLD from {url}: {e}")
            return None

    def extract_text_content(
        self, content: bytes | None, id: str
    ) -> RecordContentSchema | None:
        if not content:
            return None
        try:
            extracted = trafilatura.extract(
                content,
                include_comments=False,
                deduplicate=True,
                output_format="json",
                with_metadata=True,
            )
            if extracted:
                root = json.loads(extracted)
                return RecordContentSchema(
                    title=root.get("title"),
                    excerpt=root.get("excerpt"),
                    hostname=root.get("hostname"),
                    tags=root.get("tags"),
                    categories=root.get("categories"),
                    text=root.get("raw_text"),
                    date=root.get("date"),
                    date_crawled=root.get("filedate"),
                )
            return None
        except Exception as e:
            logger.warning(f"Error extracting text for record {id}: {e}")
            return None
