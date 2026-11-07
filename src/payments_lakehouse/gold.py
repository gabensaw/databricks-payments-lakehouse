"""Gold layer logic: FX rates, merchants, conversion to PLN and daily aggregation.

Pure transformations only: no reading or writing of tables, no widgets, no logging.
The notebook `04_gold` does the I/O and calls these functions.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from payments_lakehouse.silver import clean_text

MAX_RATE_GAP_DAYS = 3        # weekend + 1 day; a longer gap is an error, not a fallback
RATE_JUMP_WARN = 0.05        # day-over-day change above this is suspicious
GOLD_KEY = ["txn_date", "country", "category", "currency"]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def assert_unique(df: DataFrame, key: list, name: str) -> None:
    """A duplicated key in a lookup table multiplies rows after a join (fan-out)."""
    dupes = df.groupBy(*key).count().filter(F.col("count") > 1).limit(10).collect()
    check(not dupes, f"{name}: duplicated key {key}: {[r.asDict() for r in dupes]}")


# ---------- reference data ----------


def parse_rates(raw: DataFrame) -> DataFrame:
    """Raw FX CSV (strings) -> rate_date DATE, currency, rate_pln DECIMAL(12,4). Bad values become NULL."""
    return raw.select(
        F.try_to_timestamp(F.col("rate_date"), F.lit("yyyy-MM-dd")).cast("date").alias("rate_date"),
        clean_text("currency").alias("currency"),
        F.col("rate_pln").try_cast("decimal(12,4)").alias("rate_pln"),
    )


def validate_rates(rates: DataFrame) -> None:
    """Fail on missing/non-positive values and on duplicated (rate_date, currency)."""
    invalid = rates.filter(
        F.col("rate_date").isNull() | F.col("currency").isNull()
        | F.col("rate_pln").isNull() | (F.col("rate_pln") <= 0)
    ).count()
    check(invalid == 0, f"{invalid} invalid rate rows")
    assert_unique(rates, ["rate_date", "currency"], "rates")


def find_rate_jumps(rates: DataFrame, threshold: float = RATE_JUMP_WARN) -> DataFrame:
    """Rates that changed more than `threshold` since the previous published rate of the same currency."""
    previous = Window.partitionBy("currency").orderBy("rate_date")
    return (
        rates.withColumn("prev_rate", F.lag("rate_pln").over(previous))
        .filter(F.abs(F.col("rate_pln") / F.col("prev_rate") - 1) > threshold)
        .select("rate_date", "currency", "prev_rate", "rate_pln")
    )


def parse_merchants(raw: DataFrame) -> DataFrame:
    return raw.select(
        clean_text("merchant_id").alias("merchant_id"),
        F.trim("name").alias("name"),
        F.trim("category").alias("category"),
        clean_text("country").alias("country"),
    )


def validate_merchants(merchants: DataFrame) -> None:
    invalid = merchants.filter(F.col("merchant_id").isNull() | F.col("category").isNull()).count()
    check(invalid == 0, f"{invalid} merchants without id or category")
    assert_unique(merchants, ["merchant_id"], "merchants")


# ---------- transformations ----------


def build_daily_rates(rates: DataFrame, transactions: DataFrame) -> DataFrame:
    """One rate per (day, currency). Gaps take the last rate from max MAX_RATE_GAP_DAYS back; PLN = 1.

    The calendar runs until the last transaction day, so a weekend after the last published rate is covered too.
    The calendar has no gaps, so N rows back = N days back.
    """
    start = rates.agg(F.min("rate_date")).first()[0]
    end = max(
        rates.agg(F.max("rate_date")).first()[0],
        transactions.agg(F.max(F.to_date("txn_ts"))).first()[0],
    )
    calendar = (
        rates.sparkSession.createDataFrame([(start, end)], "start DATE, end DATE")
        .select(F.explode(F.sequence("start", "end")).alias("rate_date"))
    )

    last_known = Window.partitionBy("currency").orderBy("rate_date").rowsBetween(-MAX_RATE_GAP_DAYS, 0)
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


def aggregate_gold(converted: DataFrame, run_id: str) -> DataFrame:
    return (
        converted.groupBy(*GOLD_KEY)
        .agg(F.count("*").alias("txn_count"), F.sum("amount_pln").alias("turnover_pln"))
        .withColumns({"_run_id": F.lit(run_id), "_updated_ts": F.current_timestamp()})
    )


def reconcile(converted: DataFrame, gold: DataFrame) -> dict:
    """Gold totals must equal transaction-level totals."""
    source = converted.agg(F.count("*").alias("txn"), F.sum("amount_pln").alias("pln")).first()
    target = gold.agg(
        F.sum("txn_count").alias("txn"), F.sum("turnover_pln").alias("pln"), F.count("*").alias("rows")
    ).first()
    check(
        source["txn"] == target["txn"] and source["pln"] == target["pln"],
        f"Gold does not reconcile: source {source.asDict()} vs gold {target.asDict()}",
    )
    return target.asDict()
