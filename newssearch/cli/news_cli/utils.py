import logging
import re

import typer
from tqdm import tqdm

from newssearch.infrastructure.clients.news.schemas import WarcFileSchema


def configure_logging():
    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg, end="\n")
            except Exception:
                self.handleError(record)

    logging.getLogger("trafilatura").setLevel(logging.ERROR)

    root_logger = logging.getLogger()

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(TqdmLoggingHandler())


def get_files_for_download(
    files: list[WarcFileSchema],
) -> list[WarcFileSchema]:
    id_to_file_map = {p.id: p for p in files}
    available_ids = set(id_to_file_map.keys())

    while True:
        raw = process_user_input()
        try:
            start_id, end_id = parse_id_range(raw, available_ids)

            # Validate that both IDs exist
            if start_id not in available_ids:
                raise ValueError(f"Start ID {start_id} not found in available files")
            if end_id not in available_ids:
                raise ValueError(f"End ID {end_id} not found in available files")

            break
        except ValueError as exc:
            typer.echo(f"Invalid input: {exc}. Please try again.")

    start_file = id_to_file_map[start_id]
    end_file = id_to_file_map[end_id]

    start_ind = files.index(start_file)
    end_ind = files.index(end_file)

    # Ensure start index comes before end index
    if start_ind > end_ind:
        start_ind, end_ind = end_ind, start_ind
        start_id, end_id = end_id, start_id

    file_count = end_ind - start_ind + 1

    if file_count > 1:
        typer.echo(f"Loading {file_count} files from {start_id} to {end_id}...")
    else:
        typer.echo(f"Loading file {start_id}...")

    # Return the slice (inclusive of end index)
    return files[start_ind : end_ind + 1]


def process_user_input() -> str:
    try:
        raw = typer.prompt(
            "Enter ID or range (e.g. 03802 or 03802-03807), end inclusive. 'q' to quit"
        )
    except (EOFError, KeyboardInterrupt):
        typer.echo("Aborted.")
        raise typer.Exit(code=1)

    if raw.strip().lower() in {"q", "quit", "exit"}:
        typer.echo("Aborted.")
        raise typer.Exit(code=0)
    return raw


def parse_id_range(raw: str, available_ids: set[str]) -> tuple[str, str]:
    raw = raw.strip()
    if not raw:
        raise ValueError("empty input")

    # allow hyphen variants with optional spaces
    parts = re.split(r"\s*[-–—]\s*", raw)
    if len(parts) == 1:
        start_s = end_s = parts[0]
    elif len(parts) == 2:  # noqa: PLR2004
        start_s, end_s = parts
        if not start_s:
            raise ValueError("range must have a start id")
        if not end_s:
            # "03802-" treat as single id (same as start)
            end_s = start_s
    else:
        raise ValueError("too many separators")

    if not (start_s.isdigit() and end_s.isdigit()):
        raise ValueError("ids must be numeric")

    # numeric validation using available ids
    width = max(len(i) for i in available_ids)  # preserve zero-padding

    available_ints = sorted(int(i) for i in available_ids)
    min_id, max_id = available_ints[0], available_ints[-1]

    start_i, end_i = int(start_s), int(end_s)
    if start_i > end_i:
        raise ValueError("start id must be <= end id")
    if not (min_id <= start_i <= max_id and min_id <= end_i <= max_id):
        raise ValueError(
            f"ids out of available range: {min_id:0{width}}-{max_id:0{width}}"
        )

    # return zero-padded strings matching existing ids format
    return str(start_i).zfill(width), str(end_i).zfill(width)
