# Bearing Analytics

Bearing experiment data can be found at the [UTCRS Database](https://utcrsdatabase.utrgv.edu/). 

These experiments are downloadable in .zip format and should be extracted before processing.

## Project Dependencies

This project depends on uv which can be installed [here](https://docs.astral.sh/uv/getting-started/installation/).

## Ingest Experiment Data into a DuckDB Database

```shell
uv run ingest {experiment directory}
```

This will result in a duckdb database being generated in the current directory containing the following tables:

```sql
TABLE adxl_raw (
  file_id INTEGER,
  ts TIMESTAMP,
  adxl0 DOUBLE,
  adxl1 DOUBLE,
  adxl2 DOUBLE,
  adxl3 DOUBLE,
);

TABLE adxl_meta (
  id INTEGER,
  filename VARCHAR,
  start TIMESTAMP,
  skip INTEGER,
  load INTEGER,
  rpm INTEGER,
);
```

A convenience view is also provided that will compute the RMS of an ADXL sensor within a given accelerometer file and associates it with the load and rpm:

```sql
VIEW adxl_rms (
  ts TIMESTAMP,
  samples BIGINT,
  rms0 DOUBLE,
  rms1 DOUBLE,
  rms2 DOUBLE,
  rms3 DOUBLE,
  load INTEGER,
  rpm INTEGER,
  threshold DOUBLE,
);
```

This RMS view can be exported into various formats for further analysis. See the [DuckDB Documentation](https://duckdb.org/docs/2.0/sql/statements/export) for more details.

```duckdb
COPY (SELECT * FROM adxl_rms) TO "rms.csv" WITH (FORMAT csv);
```
