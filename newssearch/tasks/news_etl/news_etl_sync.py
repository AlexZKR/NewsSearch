import concurrent
import concurrent.futures
import os
import tempfile
from logging import getLogger

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newssearch.config.settings import NewsETLSettings
from newssearch.infrastructure.clients.news.news_client_sync import NewsClientSync
from newssearch.infrastructure.clients.news.schemas import WarcFileSchema
from newssearch.tasks.news_etl.base import BaseNewsETL
from newssearch.tasks.news_etl.performance.stats_collector import StatsCollector
from newssearch.tasks.news_etl.performance.timer import Timer
from newssearch.tasks.news_etl.schemas import ETLStageEnum, WARCRecordSchema
from newssearch.tasks.news_etl.utils.record_factory import (
    is_record_html,
    parse_record,
)
from newssearch.tasks.news_etl.utils.utils import (
    get_tqdm,
    write_tmp_file,
)

logger = getLogger(__name__)


class NewsETLOneThreaded(BaseNewsETL):
    """Can process multiple files. One file is processed in it's own thread.

    Work done in one thread:
    1. Download a WARC file (1 GB) to a tmpfile;
    2. Parse this file with warcio and trafilatura, extract text;
    3. Load into ElasticSearch.

    tqdm and typer for UI, run with `python cli/news_cli/news_sync.py`
    """

    def __init__(
        self, news_client: NewsClientSync, settings: NewsETLSettings = NewsETLSettings()
    ) -> None:
        self.client = news_client
        self.settings = settings

    def run(self, files: list[WarcFileSchema]):
        logger.info(f"Starting processing {len(files)} files.")
        logger.info(
            f"Max workers is {self.settings.max_workers}; Maximum {self.settings.max_workers} files can be processed at once."
        )

        with Timer(name="Overall metrics") as overall_timer:
            self.stats = StatsCollector()

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.settings.max_workers
            ) as executor:
                future_to_file = {
                    executor.submit(self.process_file, f, i): f
                    for i, f in enumerate(files)
                }

            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    future.result()
                    logger.info(f"Successfully processed {file.id}")
                except Exception as e:
                    logger.error(f"Error processing {file.id}: {e}")
                    self.stats.incr_failed_files()
                    self.stats.add_file_error(file.id, e)

        self.stats.add_overall_timing_results(overall_timer.get_timing_results())
        logger.info(f"Overall metrics:\n\n {self.stats}")

    def process_file(self, file: WarcFileSchema, position: int):
        self.stats.add_file(file.id)

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            try:
                file_size = self.extract_stage(file, tmp_file, position)
                records = self.transform_stage(tmp_file.name, file, position, file_size)
                logger.info(f"Parsed {len(records)} for file {file.id}")
                # elastic load
            finally:
                os.unlink(tmp_file.name)

    def extract_stage(
        self,
        file: WarcFileSchema,
        temp_file: tempfile._TemporaryFileWrapper,
        position: int,
    ):
        with Timer(name="Extract timer") as extract_timer:
            content_iterator = self.client.get_warc_file(file, position)
            file_size = write_tmp_file(content_iterator, temp_file)

        self.stats.add_stage_timing_results(
            file.id, ETLStageEnum.extract, extract_timer.get_timing_results()
        )
        return file_size

    def transform_stage(
        self, tmp_filepath: str, file: WarcFileSchema, pos: int, file_size: int
    ) -> list[WARCRecordSchema]:
        records = []
        with Timer(name="Transform timer") as transform_timer:
            with open(tmp_filepath, "rb") as f:
                with get_tqdm(
                    msg=f"Parsing {file.id}", total=file_size, pos=pos
                ) as pbar:
                    for record in ArchiveIterator(f):
                        current_pos = f.tell()
                        pbar.update(current_pos - pbar.n)
                        if record := self._process_record(file.id, record):
                            records.append(record)
                        else:
                            self.stats.incr_transform_failed_records(file.id)

        self.stats.add_stage_timing_results(
            file.id, ETLStageEnum.transform, transform_timer.get_timing_results()
        )
        return records

    def _process_record(
        self, file_id: str, record: ArcWarcRecord
    ) -> WARCRecordSchema | None:
        try:
            self.stats.add_tranform_record(file_id)

            if not is_record_html(record):
                self.stats.incr_transform_non_html_records(file_id)
                return None

            record_schema = parse_record(record)
            if not record_schema.content:
                self.stats.incr_transform_empty_content_records(file_id)
                return None

            return record_schema
        except Exception as exc:
            logger.warning(f"Error processing record {record}: {exc}")
            self.stats.add_transform_error(
                file_id,
                record.rec_headers.get_header("WARC-Record-ID"),
                exc,
            )
            return None
