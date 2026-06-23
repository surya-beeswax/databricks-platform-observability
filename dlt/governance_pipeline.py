import dlt
from pyspark.sql.functions import (
    current_timestamp,
    col,
    lit
)

# ============================================================
# TABLE INVENTORY
# ============================================================

@dlt.table(
    name="governance_table_inventory",
    comment="Inventory of Unity Catalog tables"
)
def governance_table_inventory():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            data_source_format,
            created,
            last_altered,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
    """)


# ============================================================
# SCHEMA INVENTORY
# ============================================================

@dlt.table(
    name="governance_schema_inventory",
    comment="Inventory of schemas"
)
def governance_schema_inventory():

    return spark.sql("""
        SELECT
            catalog_name,
            schema_name,
            current_timestamp() AS observation_time
        FROM system.information_schema.schemata
    """)


# ============================================================
# DELTA TABLE INVENTORY
# ============================================================

@dlt.table(
    name="delta_table_inventory",
    comment="Delta tables only"
)
def delta_table_inventory():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
        WHERE data_source_format='DELTA'
    """)

@dlt.table(
    name="table_access_activity",
    comment="Table access activity"
)
def table_access_activity():

    return spark.sql("""
        SELECT
            source_table_catalog AS table_catalog,
            source_table_schema AS table_schema,
            source_table_name AS table_name,
            event_date
        FROM system.access.table_lineage
    """)


@dlt.table(
    name="storage_governance_metrics",
    comment="Storage governance metrics"
)
def storage_governance_metrics():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
    """)


