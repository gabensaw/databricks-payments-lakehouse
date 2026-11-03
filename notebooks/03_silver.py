# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver — clean, validate, deduplicate
# MAGIC
# MAGIC Reads the three Bronze tables, maps them to one common schema, parses types and splits the result into:
# MAGIC - `silver_transactions` — valid, deduplicated transactions (each `txn_id` exactly once)
# MAGIC - `silver_rejected` — everything else, with `reject_reason` and the original raw values
# MAGIC
# MAGIC Rules:
# MAGIC - deduplication runs **before** validation: the newest version of a transaction wins, even if it is invalid
# MAGIC - newest = later file date, then higher file version (`_v2` > `_v1`), then the valid row
# MAGIC - after writing, the run **fails** unless `silver + rejected = bronze` and `txn_id` is unique in Silver
# MAGIC
# MAGIC Known limitation: source timestamps are local time without a time zone and are stored as-is.

# COMMAND ----------

import logging
from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

log = logging.getLogger("payments.silver")
if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    log.addHandler(_handler)
log.setLevel(logging.INFO)
log.propagate = False

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments")
dbutils.widgets.text("run_id", "manual")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RUN_ID = dbutils.widgets.get("run_id")


def full_name(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


SILVER_TABLE = full_name("silver_transactions")
REJECTED_TABLE = full_name("silver_rejected")

# COMMAND ----------

CURRENCIES = ("PLN", "EUR", "GBP")
STATUSES = ("SETTLED", "DECLINED", "REFUNDED")

BUSINESS_COLS = ["txn_id", "txn_ts", "amount", "currency", "merchant_id", "status"]
SILVER_COLS = BUSINESS_COLS + ["country", "_source_file", "_run_id", "_processed_ts"]
REJECTED_COLS = BUSINESS_COLS + [
    "raw_txn_ts", "raw_amount", "country", "_source_file",
    "reject_reason", "validation_reason", "_run_id", "_processed_ts",
]


@dataclass(frozen=True)
class SourceConfig:
    """How to turn one country's Bronze table into the common schema."""

    country: str
    ts_format: str
    decimal_sep: str
    column_map: dict = field(default_factory=dict)

    @property
    def bronze_table(self) -> str:
        return full_name(f"bronze_{self.country.lower()}")


SOURCES = (
    SourceConfig("PL", "dd.MM.yyyy HH:mm", ",", {
        "id_transakcji": "txn_id", "data_transakcji": "txn_ts", "kwota": "amount",
        "waluta": "currency", "id_sprzedawcy": "merchant_id",
    }),
    SourceConfig("DE", "yyyy-MM-dd HH:mm:ss", ".", {
        "TransaktionsID": "txn_id", "Zeitstempel": "txn_ts", "Betrag": "amount",
        "Waehrung": "currency", "HaendlerID": "merchant_id", "Status": "status",
    }),
    SourceConfig("IE", "yyyy-MM-dd'T'HH:mm:ss", "."),
)

# COMMAND ----------


def clean_text(col_name: str):
    """Trim, upper-case, empty string -> NULL."""
    return F.upper(F.nullif(F.trim(F.col(col_name)), F.lit("")))


def read_bronze(cfg: SourceConfig) -> DataFrame:
    """Read one Bronze table and map it to the common column names."""
    return (
        spark.table(cfg.bronze_table)
        .withColumnsRenamed(cfg.column_map)
        .select(*BUSINESS_COLS, "_source_file")
        .withColumn("country", F.lit(cfg.country))
    )


def parse_types(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Parse strings to types. Unparseable values become NULL (validated later); raw values are kept."""
    return df.withColumns({
        "raw_txn_ts": F.col("txn_ts"),
        "raw_amount": F.col("amount"),
        "txn_ts": F.try_to_timestamp(F.col("txn_ts"), F.lit(cfg.ts_format)),
        "amount": F.replace(F.col("amount"), F.lit(cfg.decimal_sep), F.lit(".")).try_cast("decimal(12,2)"),
        "currency": clean_text("currency"),
        "merchant_id": clean_text("merchant_id"),
        "status": clean_text("status"),
    })


def add_validation_reason(df: DataFrame) -> DataFrame:
    """First failed rule in order of importance; NULL = valid row."""
    reason = (
        F.when(F.col("txn_ts").isNull(), "invalid_date")
        .when(F.col("amount").isNull(), "invalid_amount")
        .when(F.col("amount") < 0, "negative_amount")
        .when(F.col("currency").isNull(), "missing_currency")
        .when(~F.col("currency").isin(*CURRENCIES), "wrong_currency")
        .when(F.col("merchant_id").isNull(), "missing_merchant_id")
        .when(F.col("status").isNull(), "missing_status")
        .when(~F.col("status").isin(*STATUSES), "wrong_status")
    )
    return df.withColumn("validation_reason", reason)


def resolve_duplicates(df: DataFrame) -> DataFrame:
    """Keep the newest version of each txn_id; older copies get reject_reason = 'duplicate'.

    The original validation_reason is kept, so an invalid duplicate does not lose its first problem.
    """
    newest_first = Window.partitionBy("txn_id").orderBy(
        F.desc("_source_date"),
        F.desc("_version"),
        F.desc(F.col("validation_reason").isNull()),
        F.desc("_source_file"),
    )
    return (
        df.withColumns({
            "_version": F.regexp_extract("_source_file", r"_v(\d+)\.csv$", 1).try_cast("int"),
            "_source_date": F.try_to_date(F.regexp_extract("_source_file", r"_(\d{8})", 1), F.lit("yyyyMMdd")),
        })
        .withColumn("_rn", F.row_number().over(newest_first))
        .withColumn(
            "reject_reason",
            F.when(F.col("_rn") > 1, F.lit("duplicate")).otherwise(F.col("validation_reason")),
        )
        .drop("_rn", "_version", "_source_date")
    )


def build_silver() -> DataFrame:
    """All countries in one DataFrame, every row with reject_reason (NULL = goes to Silver)."""
    prepared = [add_validation_reason(parse_types(read_bronze(cfg), cfg)) for cfg in SOURCES]
    combined = reduce(DataFrame.unionByName, prepared)
    return resolve_duplicates(combined).withColumns({
        "_run_id": F.lit(RUN_ID),
        "_processed_ts": F.current_timestamp(),
    })


def write_table(df: DataFrame, table: str) -> int:
    """Overwrite the table and return the number of written rows from the Delta log."""
    df.write.mode("overwrite").saveAsTable(table)
    metrics = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["operationMetrics"]
    return int(metrics["numOutputRows"])


def check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


# COMMAND ----------

result = build_silver()

silver_rows = write_table(result.filter(F.col("reject_reason").isNull()).select(SILVER_COLS), SILVER_TABLE)
rejected_rows = write_table(result.filter(F.col("reject_reason").isNotNull()).select(REJECTED_COLS), REJECTED_TABLE)

# COMMAND ----------

# Post-write checks — a failure here stops the Job before Gold reads bad data.
bronze_rows = sum(spark.table(cfg.bronze_table).count() for cfg in SOURCES)
check(silver_rows > 0, "Silver is empty")
check(
    silver_rows + rejected_rows == bronze_rows,
    f"Reconciliation failed: silver {silver_rows} + rejected {rejected_rows} != bronze {bronze_rows}",
)

uniqueness = spark.table(SILVER_TABLE).agg(F.count("*").alias("rows"), F.countDistinct("txn_id").alias("ids")).first()
check(uniqueness["rows"] == uniqueness["ids"], f"txn_id is not unique in Silver: {uniqueness.asDict()}")

reasons = {r["reject_reason"]: r["count"] for r in spark.table(REJECTED_TABLE).groupBy("reject_reason").count().collect()}
log.info("Silver: %d rows, rejected: %d rows, bronze: %d rows (run_id=%s)", silver_rows, rejected_rows, bronze_rows, RUN_ID)
log.info("Rejected by reason: %s", dict(sorted(reasons.items())))
