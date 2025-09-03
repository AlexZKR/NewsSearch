import concurrent
import concurrent.futures
import os
import tempfile
from logging import getLogger

from warcio.archiveiterator import ArchiveIterator

from newssearch.config.settings import NewsETLSettings
from newssearch.infrastructure.clients.news.news_client import NewsClient
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema, WarcPathsFile
from newssearch.tasks.news_etl.schemas import WARCRecordSchema
from newssearch.tasks.news_etl.utils.record_factory import (
    is_record_valid,
    process_record,
)
from newssearch.tasks.news_etl.utils.utils import (
    get_tqdm,
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
                            records.append(process_record(record))
                    except Exception as exc:
                        logger.warning(f"Error processing record {record}: {exc}")
        return records
