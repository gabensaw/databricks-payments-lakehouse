# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze — raw ingestion
# MAGIC
# MAGIC Loads raw CSV transaction files (PL / DE / IE) **1:1** into Bronze Delta tables.
# MAGIC
# MAGIC - all business columns stay **STRING** — parsing and validation happen in Silver
# MAGIC - technical columns: `_source_file`, `_file_mod_ts`, `_load_ts`, `_run_id`, `_rescued_data`
# MAGIC - full reload (`overwrite`) → re-running gives the same result (idempotent)
# MAGIC - the run **fails fast** on: missing files, empty source, unexpected header (schema drift)

# COMMAND ----------

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("payments.bronze")
if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    log.addHandler(_handler)
log.setLevel(logging.INFO)
log.propagate = False

# COMMAND ----------

# Parameters — the Job overrides them, defaults are for manual runs.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments")
dbutils.widgets.text("run_id", "manual")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RUN_ID = dbutils.widgets.get("run_id")

SOURCE_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/transactions"
RESCUED_COL = "_rescued_data"

# COMMAND ----------


@dataclass(frozen=True)
class SourceConfig:
    """Contract of one source system: how to read it and which header we expect."""

    country: str
    sep: str
    expected_columns: tuple[str, ...]
    encoding: str = "UTF-8"

    @property
    def path(self) -> str:
        return f"{SOURCE_DIR}/transactions_{self.country}_*.csv"

    @property
    def table(self) -> str:
        return f"{CATALOG}.{SCHEMA}.bronze_{self.country.lower()}"


SOURCES = (
    SourceConfig("PL", ";", ("id_transakcji", "data_transakcji", "kwota", "waluta", "id_sprzedawcy", "status")),
    SourceConfig("DE", ",", ("TransaktionsID", "Zeitstempel", "Betrag", "Waehrung", "HaendlerID", "Status")),
    SourceConfig("IE", ",", ("txn_id", "txn_ts", "amount", "currency", "merchant_id", "status")),
)

# COMMAND ----------


def read_source(cfg: SourceConfig) -> DataFrame:
    """Read all files of one country as raw strings (no type inference)."""
    return (
        spark.read
        .option("header", True)
        .option("sep", cfg.sep)
        .option("encoding", cfg.encoding)
        .option("rescuedDataColumn", RESCUED_COL)
        .csv(cfg.path)
    )


def validate_header(df: DataFrame, cfg: SourceConfig) -> None:
    """Fail when the source header differs from the contract (schema drift)."""
    actual = [c for c in df.columns if c != RESCUED_COL]
    missing = sorted(set(cfg.expected_columns) - set(actual))
    unexpected = sorted(set(actual) - set(cfg.expected_columns))
    if missing or unexpected:
        raise ValueError(
            f"[{cfg.country}] Header does not match the contract. "
            f"Missing: {missing}, unexpected: {unexpected}"
        )


def add_technical_columns(df: DataFrame) -> DataFrame:
    return df.withColumns({
        "_source_file": F.col("_metadata.file_name"),
        "_file_mod_ts": F.col("_metadata.file_modification_time"),
        "_load_ts": F.current_timestamp(),
        "_run_id": F.lit(RUN_ID),
    })


def write_table(df: DataFrame, table: str) -> int:
    """Overwrite the table and return the number of written rows from the Delta log."""
    df.write.mode("overwrite").saveAsTable(table)
    metrics = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["operationMetrics"]
    return int(metrics["numOutputRows"])


def load_bronze(cfg: SourceConfig) -> int:
    df = read_source(cfg)
    validate_header(df, cfg)

    if df.isEmpty():
        raise ValueError(f"[{cfg.country}] No rows found in {cfg.path}")

    rows = write_table(add_technical_columns(df), cfg.table)

    rescued = spark.table(cfg.table).filter(F.col(RESCUED_COL).isNotNull()).count()
    if rescued:
        log.warning("%s: %d rows have data in %s — check the source", cfg.table, rescued, RESCUED_COL)

    log.info("%s: %d rows loaded", cfg.table, rows)
    return rows


# COMMAND ----------

results = {cfg.table: load_bronze(cfg) for cfg in SOURCES}
log.info("Bronze finished. run_id=%s, total rows=%d", RUN_ID, sum(results.values()))
