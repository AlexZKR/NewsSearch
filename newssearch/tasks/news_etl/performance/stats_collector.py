import threading
from logging import getLogger

from newssearch.tasks.news_etl.performance.schemas import (
    BaseStageStats,
    ETLError,
    FileProcessingStats,
    PerFileStats,
)
from newssearch.tasks.news_etl.performance.timer import TimingResults
from newssearch.tasks.news_etl.schemas import ETLStageEnum

logger = getLogger(__name__)


class StatsCollector:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.stats: FileProcessingStats = FileProcessingStats()

    def add_file(self, file_id: str) -> None:
        with self.lock:
            logger.info(f"Starting stats collection for file {file_id}")
            self.stats.total_files += 1
            file_stats = PerFileStats(id=file_id)
            self.stats.per_file.update({file_id: file_stats})

    def add_file_error(self, file_id: str, exc: Exception) -> None:
        with self.lock:
            file_obj = self._get_file(file_id)

            error_obj = ETLError(reason=str(exc), object_id=file_id)
            file_obj.errors.append(error_obj)

    def add_stage_timing_results(
        self, file_id: str, stage: ETLStageEnum, results: TimingResults
    ) -> None:
        with self.lock:
            _, stage_stats_obj = self._get_file_and_stage(file_id, stage)
            stage_stats_obj.timing_results = results

    def add_overall_timing_results(self, results: TimingResults) -> None:
        with self.lock:
            self.stats.overall_timing_results = results

    def add_tranform_record(self, file_id: str) -> None:
        """Increment general record count for the transform stage"""
        with self.lock:
            file_stats_obj = self._get_file(file_id)
            file_stats_obj.tranform_stage_stats.total_records += 1

    def add_transform_error(self, file_id: str, object_id: str, exc: Exception) -> None:
        """For object_id usage refer to ETLError schema"""
        with self.lock:
            file_stats_obj = self._get_file(file_id)

            file_stats_obj.tranform_stage_stats.failed_records += 1

            error_obj = ETLError(reason=str(exc), object_id=object_id)
            file_stats_obj.tranform_stage_stats.errors.append(error_obj)

    def incr_failed_files(self) -> None:
        with self.lock:
            self.stats.failed_files += 1

    def incr_transform_failed_records(self, file_id: str) -> None:
        """This method should be used explicitly, not implicitly inside one of
        incr_empty_content_records, incr_non_html_records, etc.
        """
        with self.lock:
            file_stats_obj = self._get_file(file_id)
            file_stats_obj.tranform_stage_stats.failed_records += 1

    def incr_transform_empty_content_records(self, file_id: str) -> None:
        with self.lock:
            file_stats_obj = self._get_file(file_id)
            file_stats_obj.tranform_stage_stats.empty_content_records += 1

    def incr_transform_non_html_records(self, file_id: str) -> None:
        with self.lock:
            file_stats_obj = self._get_file(file_id)
            file_stats_obj.tranform_stage_stats.non_html_records += 1

    def incr_transform_lang_not_set_records(self, file_id: str) -> None:
        with self.lock:
            file_stats_obj = self._get_file(file_id)
            file_stats_obj.tranform_stage_stats.lang_not_set_records += 1

    def _get_file(self, file_id: str) -> PerFileStats:
        return self.stats.per_file[file_id]

    def _get_file_and_stage(
        self, file_id: str, stage: ETLStageEnum
    ) -> tuple[PerFileStats, BaseStageStats]:
        """Convenience for getting file and stage stats objects"""
        file_stats_obj = self._get_file(file_id)
        match stage:
            case ETLStageEnum.extract:
                stage_stats_obj = file_stats_obj.extract_stage_stats
            case ETLStageEnum.transform:
                stage_stats_obj = file_stats_obj.tranform_stage_stats
            case ETLStageEnum.load:
                stage_stats_obj = file_stats_obj.load_stage_stats
        return file_stats_obj, stage_stats_obj

    def __str__(self) -> str:
        """Return a human-friendly summary of all collected stats."""
        lines: list[str] = []
        lines.append(str(self.stats))
        for _, file_stats in self.stats.per_file.items():
            lines.append(str(file_stats))

            lines.append(str(file_stats.load_stage_stats))
            lines.append(str(file_stats.tranform_stage_stats))
            lines.append(str(file_stats.load_stage_stats))

        return "\n".join(lines)
