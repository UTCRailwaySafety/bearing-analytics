"""
ingest <experiment>
Ingests <experiment>/<idx>_<load>_<rpm>/Accelerometer*.lvm into <experiment>.db.
"""

from collections.abc import Generator, Iterable
from contextlib import contextmanager

import logging
import pathlib
import duckdb
import enum
import sys
import re


SUBDIR = re.compile(r"^(\d+)_(\d+)_(\d+)$")

SCHEMA = r"""
CREATE SEQUENCE IF NOT EXISTS adxl_file_id;

CREATE OR REPLACE TABLE adxl_raw (
    file_id INTEGER NOT NULL,
    ts TIMESTAMP NOT NULL,
    adxl0 DOUBLE NOT NULL,
    adxl1 DOUBLE NOT NULL,
    adxl2 DOUBLE NOT NULL,
    adxl3 DOUBLE NOT NULL,
);

CREATE OR REPLACE TABLE adxl_meta (
    id INTEGER PRIMARY KEY DEFAULT nextval('adxl_file_id'),
    filename VARCHAR NOT NULL UNIQUE,
    start TIMESTAMP NOT NULL,
    skip INTEGER NOT NULL,
    load INTEGER NOT NULL,
    rpm INTEGER NOT NULL,
);

CREATE OR REPLACE VIEW adxl_rms AS
WITH files AS (
    SELECT
        raw.file_id,
        min(raw.ts) AS ts,
        count(*) AS samples,
        25.0 * stddev_pop(raw.adxl0) AS rms0,
        25.0 * stddev_pop(raw.adxl1) AS rms1,
        25.0 * stddev_pop(raw.adxl2) AS rms2,
        25.0 * stddev_pop(raw.adxl3) AS rms3,
    FROM adxl_raw AS raw
    GROUP BY raw.file_id
)
SELECT
    files.ts,
    files.samples,
    files.rms0,
    files.rms1,
    files.rms2,
    files.rms3,
    meta.load,
    meta.rpm,
    CASE WHEN meta.rpm = 0.0 THEN 0.0 ELSE meta.rpm * 0.0113::DOUBLE - 0.839::DOUBLE END AS threshold,
FROM files
ASOF JOIN adxl_meta AS meta ON files.ts >= meta.start
ORDER BY files.ts;

CREATE OR REPLACE TEMP TABLE adxl_stage (
    filename VARCHAR,
    start TIMESTAMP,
    skip INTEGER,
    load INTEGER,
    rpm INTEGER,
);
"""

REGISTER = r"""
INSERT INTO adxl_meta (filename, start, skip, load, rpm)
SELECT filename, start, skip, load, rpm FROM adxl_stage
ON CONFLICT (filename) DO NOTHING
RETURNING filename, skip;
"""

INSERT = r"""
INSERT INTO adxl_raw
SELECT
    meta.id AS file_id,
    meta.start + to_microseconds(CAST(round(lvm.delta * 1e6) AS BIGINT)) AS ts,
    lvm.adxl0,
    lvm.adxl1,
    lvm.adxl2,
    lvm.adxl3,
FROM read_csv($files, delim='\t', header=false, skip=$skip, null_padding=true, auto_detect=false, filename=true, columns={
    'delta': 'DOUBLE',
    'adxl0': 'DOUBLE',
    'adxl1': 'DOUBLE',
    'adxl2': 'DOUBLE',
    'adxl3': 'DOUBLE',
    'adxl4': 'DOUBLE',
}) AS lvm
JOIN adxl_meta AS meta USING (filename);
"""


log = logging.getLogger(__name__)


class Exit(enum.IntEnum):
    OK = 0
    NO_INPUT = 1
    DB_ERROR = 2


@contextmanager
def transaction(conn: duckdb.DuckDBPyConnection) -> Generator[duckdb.DuckDBPyConnection]:
    _ = conn.begin()
    try:
        yield conn
    except BaseException:
        _ = conn.rollback()
        raise
    else:
        _ = conn.commit()


@contextmanager
def progress(conn: duckdb.DuckDBPyConnection, delay_ms: int = 100) -> Generator[duckdb.DuckDBPyConnection]:
    enabled, delay = conn.execute(r"SELECT current_setting('enable_progress_bar'), current_setting('progress_bar_time')").fetchone() or (False, delay_ms)

    _ = conn.execute(f"SET progress_bar_time = {int(delay_ms)}")
    _ = conn.execute(r"SET enable_progress_bar = true")

    try:
        yield conn
    finally:
        _ = conn.execute(f"SET progress_bar_time = {int(delay)}")
        _ = conn.execute(f"SET enable_progress_bar = {str(enabled).lower()}")


def parse_lvm_header(header: Iterable[str]) -> tuple[str, int] | None: 
    date = time = None

    for skip, line in enumerate(header, start=1):
        key, _, rest = line.partition("\t")

        if key == "Date" and date is None:
            date = rest.split("\t")[0].strip()

        elif key == "Time" and time is None:
            time = rest.split("\t")[0].strip()

        elif key == "X_Value":
            break

    else:
        return None

    if date is None or time is None:
        return None

    hms, _, frac = time.partition(".")

    return f"{date.replace('/', '-')} {hms}.{frac[:6]:0<6}", skip


def bulk_insert(conn: duckdb.DuckDBPyConnection, meta: list[tuple[str, str, int, int, int]]) -> None:
    with transaction(conn):
        _ = conn.executemany("INSERT INTO adxl_stage VALUES (?, ?, ?, ?, ?)", meta)
        staged: list[tuple[str, int]] = conn.execute(REGISTER).fetchall()

        batch: dict[int, list[str]] = {}
        for file, skip, in staged:
            batch.setdefault(skip, []).append(file)

        with progress(conn):
            for skip, files in batch.items():
                _ = conn.execute(INSERT, {"files": files, "skip": skip})


def ingest_experiment(conn: duckdb.DuckDBPyConnection, root: pathlib.Path) -> Exit:
    meta: list[tuple[str, str, int, int, int]] = []
    for run in sorted(root.iterdir()):
        sub = SUBDIR.match(run.name)

        if not (run.is_dir() and sub):
            log.info("skipping invalid directory - %s", str(run))
            continue

        _, load, rpm = map(int, sub.groups())

        for lvm in sorted(run.glob("Accelerometer*.lvm")):
            with open(lvm, "r", encoding="utf8", errors="replace") as file:
                header = file.read(4096).splitlines()

            if (parsed := parse_lvm_header(header)) is None:
                log.error("invalid header for file %s", str(lvm)) 
                continue

            time, skip = parsed
            meta.append((str(lvm), time, skip, load, rpm))

    if not meta:
        log.error("%s: no Accelerometer lvm files with valid headers", root.name)
        return Exit.NO_INPUT

    try:
        bulk_insert(conn, meta)
    except duckdb.Error:
        log.exception("%s: ingest failed, rolled back", root.name)
        return Exit.DB_ERROR

    log.info("%s ingested %d files successfully.", root.name, len(meta))

    return Exit.OK


def ingest() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 1

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    root = pathlib.Path(sys.argv[1]).resolve()

    with duckdb.connect(f"{root.name}.db") as conn:
        _ = conn.execute(SCHEMA)
        return ingest_experiment(conn, root)
