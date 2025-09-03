import concurrent
import concurrent.futures
import json
import os
import tempfile
from datetime import date
from logging import getLogger

import trafilatura
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newssearch.config.settings import NewsETLSettings
from newssearch.infrastructure.clients.news.exceptions import FileNotFound
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema, WarcPathsFile
from newssearch.tasks.news_etl.schemas import RecordContentSchema, WARCRecordSchema
from newssearch.tasks.news_etl.utils import (
    extract_top_level_domain,
    format_year_month,
    get_tqdm,
    is_record_valid,
    write_tmp_file,
)

logger = getLogger(__name__)


class NewsETL:
    def __init__(
        self, news_client: NewsClient, settings: NewsETLSettings = NewsETLSettings()
    ) -> None:
        self.client = news_client
        self.__paths_files: dict[str, WarcPathsFile] = {}
        self.settings = settings

    def run(self, files: list[WarcFileSchema]):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.settings.max_workers
        ) as executor:
            future_to_file = {
                executor.submit(self.process_file, f, i): f for i, f in enumerate(files)
            }

        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            try:
                future.result()
                logger.info(f"Successfully processed {file.id}")
            except Exception as e:
                logger.error(f"Error processing {file.id}: {e}")

    def process_file(self, file: WarcFileSchema, position: int):
        content_iterator = self.client.get_warc_file(file, position)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            file_size = write_tmp_file(content_iterator, temp_file)
            try:
                records = self.transform_warc(temp_file.name, file, position, file_size)
                logger.info(f"Parsed {len(records)} for file {file.id}")

                # elastic load
            finally:
                os.unlink(temp_file.name)

    def transform_warc(
        self, tmp_filepath: str, file: WarcFileSchema, pos: int, file_size: int
    ) -> list[WARCRecordSchema]:
        records = []
        with open(tmp_filepath, "rb") as f:
            with get_tqdm(msg=f"Parsing {file.id}", total=file_size, pos=pos) as pbar:
                for record in ArchiveIterator(f):
                    current_pos = f.tell()
                    pbar.update(current_pos - pbar.n)
                    try:
                        if is_record_valid(record):
                            records.append(self._process_record(record))
                    except Exception as exc:
                        logger.warning(f"Error processing record {record}: {exc}")
        return records

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

    def _process_record(self, record: ArcWarcRecord):
        return WARCRecordSchema(
            id=record.rec_headers.get_header("WARC-Record-ID"),
            url=extract_top_level_domain(
                record.rec_headers.get_header("WARC-Target-URI")
            ),
            date=record.rec_headers.get_header("WARC-Date"),
            content_length=record.rec_headers.get_header("Content-Length"),
            mime_type=record.http_headers.get_header("Content-Type")
            if record.http_headers
            else None,
            content=self._extract_text_content(record.content_stream().read()),
        )

    def _extract_text_content(
        self, content: bytes | None
    ) -> RecordContentSchema | None:
        if not content:
            return None
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
