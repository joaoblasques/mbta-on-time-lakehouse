"""Gold: turn per-stop lateness into On-Time Performance. "On time" is a product decision — a
configurable band [early, late] in minutes. classify() tags each observation; otp_agg() rolls up
OTP% over any dimensions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EARLY_BOUND, LATE_BOUND = -1.0, 5.0  # minutes: < early = early, > late = late, else on-time


def classify(df: DataFrame, early: float = EARLY_BOUND, late: float = LATE_BOUND) -> DataFrame:
    """Add on_time / is_late / is_early (0/1) + hour-of-day from actual_secs_adj. Needs
    lateness_min (and actual_secs_adj for hour)."""
    return (df.filter(F.col("lateness_min").isNotNull())
            .withColumn("on_time", ((F.col("lateness_min") >= early)
                                    & (F.col("lateness_min") <= late)).cast("int"))
            .withColumn("is_late", (F.col("lateness_min") > late).cast("int"))
            .withColumn("is_early", (F.col("lateness_min") < early).cast("int"))
            .withColumn("hour", (F.floor(F.col("actual_secs_adj") / 3600) % 24).cast("int")))


def otp_agg(df: DataFrame, dims: list[str]) -> DataFrame:
    """OTP rollup over `dims`. Expects classify()'d input."""
    return (df.groupBy(*dims)
            .agg(F.count("*").alias("observations"),
                 F.sum("on_time").alias("on_time_n"),
                 F.sum("is_late").alias("late_n"),
                 F.sum("is_early").alias("early_n"),
                 F.round(100.0 * F.sum("on_time") / F.count("*"), 1).alias("otp_pct"),
                 F.round(F.avg("lateness_min"), 1).alias("avg_late_min"),
                 F.round(F.expr("percentile(lateness_min, 0.5)"), 1).alias("median_late_min")))
