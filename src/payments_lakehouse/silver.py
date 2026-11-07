"""Silver layer logic: map Bronze sources to one schema, parse types, validate and deduplicate."""

from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


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


def clean_text(col_name: str) -> Column:
    """Trim, upper-case, empty string -> NULL."""

    return F.upper(F.nullif(F.trim(F.col(col_name)), F.lit("")))


def standardize(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Rename source columns to the common names and add the country code."""

    renamed_df = df.withColumnsRenamed(cfg.column_map)
    renamed_df = renamed_df.select(*BUSINESS_COLS, "_source_file")
    standardized_df = renamed_df.withColumn("country", F.lit(cfg.country))

    return standardized_df


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
            "_source_date": F.try_to_timestamp(F.regexp_extract("_source_file", r"_(\d{8})", 1), F.lit("yyyyMMdd")),
        })
        .withColumn("_rn", F.row_number().over(newest_first))
        .withColumn(
            "reject_reason",
            F.when(F.col("_rn") > 1, F.lit("duplicate")).otherwise(F.col("validation_reason")),
        )
        .drop("_rn", "_version", "_source_date")
    )


def prepare_source(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Rename columns, parse types, and add validation_reason."""
    standardized = standardize(df, cfg)
    typed = parse_types(standardized, cfg)

    return add_validation_reason(typed)


def build_silver(bronze: dict[str, DataFrame], run_id: str, sources: tuple[SourceConfig, ...]) -> DataFrame:
    """All countries in one DataFrame, every row with reject_reason (NULL = goes to Silver)."""
    
    missing = [cfg.country for cfg in sources if cfg.country not in bronze]
    if missing:
        raise ValueError(f"Missing Bronze data for: {missing}")
    
    prepared = [prepare_source(bronze[cfg.country], cfg) for cfg in sources]

    combined = reduce(DataFrame.unionByName, prepared)

    return resolve_duplicates(combined).withColumns({
        "_run_id": F.lit(run_id),
        "_processed_ts": F.current_timestamp(),
    })
