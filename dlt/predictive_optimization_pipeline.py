import dlt
from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# ============================================================
# TABLE INVENTORY
# ============================================================

@dlt.table(
    name="table_inventory",
    comment="Inventory of Delta tables"
)
def table_inventory():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            data_source_format,
            created,
            last_altered,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
        WHERE data_source_format = 'DELTA'
    """)


# ============================================================
# PREDICTIVE OPTIMIZATION STATUS
# ============================================================

@dlt.table(
    name="predictive_optimization_status",
    comment="Tables monitored for predictive optimization"
)
def predictive_optimization_status():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
        WHERE data_source_format='DELTA'
    """)


# ============================================================
# STORAGE GROWTH MONITORING
# ============================================================

@dlt.table(
    name="storage_growth_metrics",
    comment="Storage growth observations"
)
def storage_growth_metrics():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
        WHERE data_source_format='DELTA'
    """)


# ============================================================
# OPTIMIZATION CANDIDATES
# ============================================================

@dlt.table(
    name="optimization_candidates",
    comment="Tables that may benefit from predictive optimization"
)
def optimization_candidates():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            current_timestamp() AS recommendation_time
        FROM system.information_schema.tables
        WHERE data_source_format='DELTA'
    """)

@dlt.table(
    name="table_access_summary"
)
def table_access_summary():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            COUNT(*) AS query_count
        FROM system.access.table_lineage
        GROUP BY
            table_catalog,
            table_schema,
            table_name
    """)


@dlt.table(
    name="predictive_optimization_recommendations"
)
def predictive_optimization_recommendations():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            query_count,

            CASE
                WHEN query_count > 1000
                    THEN 'HIGH_PRIORITY'

                WHEN query_count > 100
                    THEN 'MEDIUM_PRIORITY'

                ELSE 'LOW_PRIORITY'
            END AS recommendation
        FROM LIVE.table_access_summary
    """)

