# Databricks notebook source
# MAGIC %md
# MAGIC # 04_gold — FX conversion and daily turnover
# MAGIC
# MAGIC 1. Loads FX rates (`fx_rates`) and merchants (`merchants`) from the volume, with quality checks.
# MAGIC 2. Builds a daily rate calendar: missing days (weekends) take the last known rate, **max 3 days back**; PLN = 1.
# MAGIC 3. Converts SETTLED transactions to PLN and aggregates
# MAGIC    **day × country × merchant category × currency → `txn_count`, `turnover_pln`**.
# MAGIC 4. Upserts `gold_daily_turnover` with **MERGE** — only rows whose numbers changed are rewritten.
# MAGIC
# MAGIC Fails on: unexpected CSV header, invalid or duplicated rates, missing rate for a transaction,
# MAGIC unknown merchant, duplicated merchant, Gold totals that do not reconcile (before and after the MERGE).
# MAGIC Warns on: day-over-day rate change above 5 %.
# MAGIC
# MAGIC The source of the MERGE is always the **full** Gold recomputed from Silver, so the MERGE also deletes
# MAGIC keys that no longer exist (`WHEN NOT MATCHED BY SOURCE THEN DELETE`).
# MAGIC ⚠️ If you ever MERGE only a subset of days, that delete must be limited to the same days.

# COMMAND ----------

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

log = logging.getLogger("payments.gold")
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
dbutils.widgets.text("rates_file", "fx_rates_nbp.csv")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RUN_ID = dbutils.widgets.get("run_id")
RATES_FILE = dbutils.widgets.get("rates_file")


def full_name(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
RATES_PATH = f"{VOLUME}/fx_rates/{RATES_FILE}"
MERCHANTS_PATH = f"{VOLUME}/reference/merchants.csv"

SILVER_TABLE = full_name("silver_transactions")
RATES_TABLE = full_name("fx_rates")
MERCHANTS_TABLE = full_name("merchants")
GOLD_TABLE = full_name("gold_daily_turnover")

MAX_RATE_GAP_DAYS = 3
RATE_JUMP_WARN = 0.05
GOLD_KEY = ["txn_date", "country", "category", "currency"]

# COMMAND ----------


def clean_text(col_name: str):
    """Trim, upper-case, empty string -> NULL (same rule as in Silver, so join keys match)."""
    return F.upper(F.nullif(F.trim(F.col(col_name)), F.lit("")))


def check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def write_table(df: DataFrame, table: str) -> int:
    """Overwrite the table and return the number of written rows from the Delta log."""
    df.write.mode("overwrite").saveAsTable(table)
    metrics = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["operationMetrics"]
    return int(metrics["numOutputRows"])


def read_csv(path: str, expected_columns: tuple) -> DataFrame:
    """Read a CSV with header as strings; fail if the header differs from the contract."""
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(path)
    check(
        set(df.columns) == set(expected_columns),
        f"{path}: header {df.columns} does not match the contract {list(expected_columns)}",
    )
    return df


def assert_unique(df: DataFrame, key: list, name: str) -> None:
    """A duplicated key in a lookup table multiplies rows after a join (fan-out)."""
    dupes = df.groupBy(*key).count().filter(F.col("count") > 1).limit(10).collect()
    check(not dupes, f"{name}: duplicated key {key}: {[r.asDict() for r in dupes]}")


# COMMAND ----------


def warn_rate_jumps(rates: DataFrame) -> None:
    """Log rates that changed more than RATE_JUMP_WARN since the previous published day."""
    previous = Window.partitionBy("currency").orderBy("rate_date")
    jumps = (
        rates.withColumn("prev_rate", F.lag("rate_pln").over(previous))
        .filter(F.abs(F.col("rate_pln") / F.col("prev_rate") - 1) > RATE_JUMP_WARN)
        .select("rate_date", "currency", "prev_rate", "rate_pln")
        .collect()
    )
    for r in jumps:
        log.warning("Suspicious rate %s %s: %s -> %s", r.currency, r.rate_date, r.prev_rate, r.rate_pln)


def load_rates(path: str) -> DataFrame:
    raw = read_csv(path, ("rate_date", "currency", "rate_pln"))
    rates = raw.select(
        F.try_to_date(F.col("rate_date"), "yyyy-MM-dd").alias("rate_date"),
        clean_text("currency").alias("currency"),
        F.col("rate_pln").try_cast("decimal(12,4)").alias("rate_pln"),
    )

    invalid = rates.filter(
        F.col("rate_date").isNull() | F.col("currency").isNull()
        | F.col("rate_pln").isNull() | (F.col("rate_pln") <= 0)
    ).count()
    check(invalid == 0, f"{path}: {invalid} invalid rate rows")
    assert_unique(rates, ["rate_date", "currency"], "rates")
    warn_rate_jumps(rates)

    rows = write_table(rates, RATES_TABLE)
    log.info("%s: %d rows loaded from %s", RATES_TABLE, rows, RATES_FILE)
    return spark.table(RATES_TABLE)


def load_merchants(path: str) -> DataFrame:
    raw = read_csv(path, ("merchant_id", "name", "category", "country"))
    merchants = raw.select(
        clean_text("merchant_id").alias("merchant_id"),
        F.trim("name").alias("name"),
        F.trim("category").alias("category"),
        clean_text("country").alias("country"),
    )

    invalid = merchants.filter(F.col("merchant_id").isNull() | F.col("category").isNull()).count()
    check(invalid == 0, f"{path}: {invalid} merchants without id or category")
    assert_unique(merchants, ["merchant_id"], "merchants")

    rows = write_table(merchants, MERCHANTS_TABLE)
    log.info("%s: %d rows loaded", MERCHANTS_TABLE, rows)
    return spark.table(MERCHANTS_TABLE)


# COMMAND ----------


def build_daily_rates(rates: DataFrame, transactions: DataFrame) -> DataFrame:
    """One rate per (day, currency). Gaps take the last rate from max MAX_RATE_GAP_DAYS back; PLN = 1.

    The calendar runs until the last transaction day, so a weekend after the last published rate is covered too.
    """
    start = rates.agg(F.min("rate_date")).first()[0]
    end = max(
        rates.agg(F.max("rate_date")).first()[0],
        transactions.agg(F.max(F.to_date("txn_ts"))).first()[0],
    )
    calendar = (
        spark.createDataFrame([(start, end)], "start DATE, end DATE")
        .select(F.explode(F.sequence("start", "end")).alias("rate_date"))
    )

    last_known = (
        Window.partitionBy("currency").orderBy("rate_date").rowsBetween(-MAX_RATE_GAP_DAYS, 0)
    )
    foreign = (
        calendar.crossJoin(rates.select("currency").distinct())
        .join(rates, on=["rate_date", "currency"], how="left")
        .withColumn("rate_pln", F.last("rate_pln", ignorenulls=True).over(last_known))
    )
    pln = calendar.select(
        "rate_date",
        F.lit("PLN").alias("currency"),
        F.lit(1).cast("decimal(12,4)").alias("rate_pln"),
    )
    return foreign.unionByName(pln)


def convert_to_pln(transactions: DataFrame, daily_rates: DataFrame, merchants: DataFrame) -> DataFrame:
    """SETTLED transactions with txn_date, category and amount_pln. Fails on a missing rate or merchant."""
    enriched = (
        transactions.filter(F.col("status") == "SETTLED")
        .withColumn("txn_date", F.to_date("txn_ts"))
        .join(daily_rates.withColumnRenamed("rate_date", "txn_date"), on=["txn_date", "currency"], how="left")
        .join(merchants.select("merchant_id", "category"), on="merchant_id", how="left")
    )

    no_rate = enriched.filter(F.col("rate_pln").isNull()).select("currency", "txn_date").distinct().limit(20).collect()
    check(not no_rate, f"Missing FX rate for: {[r.asDict() for r in no_rate]}")

    no_category = enriched.filter(F.col("category").isNull()).select("merchant_id").distinct().limit(20).collect()
    check(not no_category, f"Unknown merchants: {[r.merchant_id for r in no_category]}")

    return enriched.withColumn(
        "amount_pln",
        F.bround(F.col("amount") * F.col("rate_pln"), 2).cast("decimal(18,2)"),  # banker's rounding
    )


def aggregate_gold(converted: DataFrame) -> DataFrame:
    return (
        converted.groupBy(*GOLD_KEY)
        .agg(F.count("*").alias("txn_count"), F.sum("amount_pln").alias("turnover_pln"))
        .withColumns({"_run_id": F.lit(RUN_ID), "_updated_ts": F.current_timestamp()})
    )


def reconcile(converted: DataFrame, gold: DataFrame) -> dict:
    """Gold totals must equal transaction-level totals."""
    source = converted.agg(F.count("*").alias("txn"), F.sum("amount_pln").alias("pln")).first()
    target = gold.agg(F.sum("txn_count").alias("txn"), F.sum("turnover_pln").alias("pln"), F.count("*").alias("rows")).first()
    check(
        source["txn"] == target["txn"] and source["pln"] == target["pln"],
        f"Gold does not reconcile: source {source.asDict()} vs gold {target.asDict()}",
    )
    return target.asDict()


# COMMAND ----------

GOLD_DDL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    txn_date     DATE          NOT NULL,
    country      STRING        NOT NULL,
    category     STRING        NOT NULL,
    currency     STRING        NOT NULL,
    txn_count    BIGINT        NOT NULL,
    turnover_pln DECIMAL(28,2) NOT NULL,
    _run_id      STRING,
    _updated_ts  TIMESTAMP
)
COMMENT 'Daily SETTLED turnover in PLN by country, merchant category and currency'
"""

GOLD_MERGE = f"""
MERGE INTO {GOLD_TABLE} AS t
USING gold_updates AS s
ON  t.txn_date = s.txn_date
AND t.country  = s.country
AND t.category = s.category
AND t.currency = s.currency
WHEN MATCHED AND (
       t.txn_count    IS DISTINCT FROM s.txn_count
    OR t.turnover_pln IS DISTINCT FROM s.turnover_pln
) THEN UPDATE SET
    t.txn_count    = s.txn_count,
    t.turnover_pln = s.turnover_pln,
    t._run_id      = s._run_id,
    t._updated_ts  = s._updated_ts
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
"""


def merge_gold(gold: DataFrame) -> dict:
    spark.sql(GOLD_DDL)
    gold.createOrReplaceTempView("gold_updates")
    return spark.sql(GOLD_MERGE).first().asDict()


# COMMAND ----------

rates = load_rates(RATES_PATH)
merchants = load_merchants(MERCHANTS_PATH)
silver = spark.table(SILVER_TABLE)

converted = convert_to_pln(silver, build_daily_rates(rates, silver), merchants)
gold = aggregate_gold(converted)

totals = reconcile(converted, gold)                          # before writing: aggregation is correct
merge_metrics = merge_gold(gold)
reconcile(converted, spark.table(GOLD_TABLE))               # after writing: the table holds exactly that

log.info("Gold: %d keys, %d SETTLED transactions, %s PLN", totals["rows"], totals["txn"], totals["pln"])
log.info("MERGE: %s", merge_metrics)
