# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver — clean, validate, deduplicate
# MAGIC
# MAGIC Thin I/O layer: reads the Bronze tables, calls the tested logic from `src/payments_lakehouse/silver.py`,
# MAGIC writes `silver_transactions` and `silver_rejected`, and runs the post-write checks.
# MAGIC
# MAGIC The transformation rules (mapping, parsing, validation, deduplication) live in the module and are covered
# MAGIC by unit tests in `tests/test_silver.py`.

# COMMAND ----------

import logging
import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Git folder: the notebook runs inside notebooks/, the package lives in ../src
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from payments_lakehouse.silver import REJECTED_COLS, SILVER_COLS, SOURCES, build_silver

# COMMAND ----------

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
BRONZE_TABLES = {cfg.country: full_name(f"bronze_{cfg.country.lower()}") for cfg in SOURCES}

# COMMAND ----------


def write_table(df: DataFrame, table: str) -> int:
    """Overwrite the table and return the number of written rows from the Delta log."""
    df.write.mode("overwrite").saveAsTable(table)
    metrics = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["operationMetrics"]
    return int(metrics["numOutputRows"])


def check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


# COMMAND ----------

bronze = {country: spark.table(table) for country, table in BRONZE_TABLES.items()}
result = build_silver(bronze, RUN_ID, SOURCES)

silver_rows = write_table(result.filter(F.col("reject_reason").isNull()).select(SILVER_COLS), SILVER_TABLE)
rejected_rows = write_table(result.filter(F.col("reject_reason").isNotNull()).select(REJECTED_COLS), REJECTED_TABLE)

# COMMAND ----------

# Post-write checks — a failure here stops the Job before Gold reads bad data.
bronze_rows = sum(spark.table(table).count() for table in BRONZE_TABLES.values())

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
