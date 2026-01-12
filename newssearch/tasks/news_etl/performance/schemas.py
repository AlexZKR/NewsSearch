from pydantic import BaseModel, Field

from newssearch.tasks.news_etl.performance.timer import TimingResults
from newssearch.tasks.news_etl.schemas import ETLStageEnum


class ETLError(BaseModel):
    """This is generic error component.

    It's should be used like what (reason) and where (object_id),
    which can be either file_id or record_id or whatever.
    """

    reason: str
    object_id: str

    def __str__(self) -> str:
        return f"  - Object: {self.object_id}, Reason: {self.reason}\n"


class BaseStageStats(BaseModel):
    stage: ETLStageEnum = ETLStageEnum.transform
    timing_results: TimingResults | None = None
    errors: list[ETLError] = Field(default_factory=list)

    def __str__(self) -> str:
        timing_info = ""
        if self.timing_results:
            timing_info = f"Timing: {self.timing_results}\n"

        return (
            f"\n--- Stage {self.stage.value} ---\n"
            f"Errors: {len(self.errors)}\n\n"
            f"{timing_info}"
            "Error details:\n"
            "".join([str(err) for err in self.errors])
        )


class ExtractStageStats(BaseStageStats): ...


class LoadStageStats(BaseStageStats): ...


class TransformStageStats(BaseStageStats):
    total_records: int = 0
    failed_records: int = 0

    empty_content_records: int = 0
    non_html_records: int = 0
    lang_not_set_records: int = 0

    @property
    def successful_records(self) -> int:
        return self.total_records - self.failed_records

    @property
    def percent_of_successful(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.successful_records / self.total_records) * 100

    def __str__(self) -> str:
        base = super().__str__()

        return base.join(
            "\nTranform stage stats:\n"
            f"Total records: {self.total_records}\n"
            f"Failed records: {self.failed_records}\n"
            f"Successful records: {self.successful_records}\n"
            f"% of successful: {self.percent_of_successful:%}\n\n"
            "Failed records details:\n"
            f"Empty content: {self.empty_content_records}\n"
            f"Non-HTML records: {self.non_html_records}\n"
            f"Lang not set: not implemented\n\n"
        )


class PerFileStats(BaseModel):
    id: str

    extract_stage_stats: ExtractStageStats = ExtractStageStats()
    tranform_stage_stats: TransformStageStats = TransformStageStats()
    load_stage_stats: LoadStageStats = LoadStageStats()

    errors: list[ETLError] = Field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"\n--- File {self.id} ---\n"
            f"Errors: {len(self.errors)}\n\n"
            "Error details:\n"
            "".join([str(err) for err in self.errors])
        )


class FileProcessingStats(BaseModel):
    total_files: int = 0
    failed_files: int = 0
    overall_timing_results: TimingResults | None = None
    per_file: dict[str, PerFileStats] = Field(default_factory=dict)

    def __str__(self) -> str:
        return (
            f"=== ETL Stats Summary ===\n\n"
            f"Total files: {self.total_files}\n"
            f"Failed files: {self.failed_files}\n"
            f"Timing:\n"
            f"{str(self.overall_timing_results)}"
        )
