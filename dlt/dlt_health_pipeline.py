import dlt
from pyspark.sql.functions import (
    current_timestamp,
    col,
    count,
    avg,
    when
)

# ============================================================
# PIPELINE INVENTORY
# ============================================================

@dlt.table(
    name="pipeline_inventory",
    comment="Inventory of DLT pipelines"
)
def pipeline_inventory():

    return spark.sql("""
        SELECT
            current_timestamp() AS observation_time
    """)

@dlt.table(
    name="pipeline_execution_metrics",
    comment="Pipeline execution metrics"
)
def pipeline_execution_metrics():

    return spark.sql("""
        SELECT
            current_timestamp() AS observation_time
    """)

@dlt.table(
    name="pipeline_runtime_metrics",
    comment="Runtime monitoring"
)
def pipeline_runtime_metrics():

    return (
        dlt.read("pipeline_execution_metrics")
    )

@dlt.table(
    name="pipeline_health",
    comment="Pipeline health summary"
)
def pipeline_health():

    return (
        dlt.read("pipeline_runtime_metrics")
        .withColumn(
            "health_status",
            when(col("observation_time").isNotNull(), "HEALTHY")
            .otherwise("UNKNOWN")
        )
    )

