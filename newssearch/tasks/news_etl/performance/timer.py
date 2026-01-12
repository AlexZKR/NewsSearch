import time

import psutil
from pydantic import BaseModel

from newssearch.tasks.news_etl.performance.utils import MB


class TimingResults(BaseModel):
    name: str
    human_time_format: str = "%H:%M:%S"

    wall_time: float
    cpu_time: float
    thread_time: float

    rss_before: int
    rss_after: int

    @property
    def wall_time_human(self) -> str:
        return time.strftime(self.human_time_format, time.gmtime(self.wall_time))

    @property
    def cpu_time_human(self) -> str:
        return time.strftime(self.human_time_format, time.gmtime(self.cpu_time))

    @property
    def thread_time_human(self) -> str:
        return time.strftime(self.human_time_format, time.gmtime(self.thread_time))

    @property
    def rss_before_mb(self) -> str:
        return f"{self.rss_before / MB:.2f} MB"

    @property
    def rss_after_mb(self) -> str:
        return f"{self.rss_after / MB:.2f} MB"

    def __str__(self) -> str:
        return (
            f"Timer: {self.name}\n"
            f"Wall Time: {self.wall_time_human}\n"
            f"CPU Time: {self.cpu_time_human}\n"
            f"Thread Time: {self.thread_time_human}\n"
            f"RSS Before: {self.rss_before_mb}\n"
            f"RSS After: {self.rss_after_mb}\n"
            f"Memory Increase: {float(self.rss_after - self.rss_before) / MB:.2f} MB"
        )


class Timer:
    """Measure wall time, cpu time and take RSS snapshots before/after."""

    def __init__(self, name: str = ""):
        self.name = name

    def __enter__(self):
        proc = psutil.Process()
        self.rss_before = proc.memory_info().rss

        self.wall_start_time = time.perf_counter()
        self.cpu_start_time = time.process_time()
        self.thread_start_time = time.thread_time()
        return self

    def __exit__(self, exc_type, exc, tb):
        proc = psutil.Process()
        self.rss_after = proc.memory_info().rss

        self.wall_end_time = time.perf_counter() - self.wall_start_time
        self.cpu_end_time = time.process_time() - self.cpu_start_time
        self.thread_end_time = time.thread_time() - self.thread_start_time

    def get_timing_results(self) -> TimingResults:
        return TimingResults(
            name=self.name,
            wall_time=self.wall_end_time,
            cpu_time=self.cpu_end_time,
            thread_time=self.thread_end_time,
            rss_before=self.rss_before,
            rss_after=self.rss_after,
        )

    def __str__(self) -> str:
        model = self.get_timing_results()
        return (
            f"Timer: {model.name}\n"
            f"Wall Time: {model.wall_time_human}\n"
            f"CPU Time: {model.cpu_time_human}\n"
            f"RSS Before: {model.rss_before_mb}\n"
            f"RSS After: {model.rss_after_mb}\n"
            f"Memory Increase: {float(model.rss_after - model.rss_before) / MB:.2f} MB"
        )
