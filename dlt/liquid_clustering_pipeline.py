import dlt
from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# --------------------------------------------------------------------
# Inventory of all tables
# --------------------------------------------------------------------

@dlt.table(
    name="table_inventory",
    comment="Inventory of tables in Unity Catalog"
)
def table_inventory():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            data_source_format,
            storage_path,
            created,
            last_altered
        FROM system.information_schema.tables
        WHERE table_type='MANAGED'
    """)


# --------------------------------------------------------------------
# Liquid Clustering Metadata
# --------------------------------------------------------------------

@dlt.table(
    name="liquid_clustering_inventory",
    comment="Liquid clustered tables"
)
def liquid_clustering_inventory():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            clustering_columns,
            current_timestamp() AS observation_time
        FROM system.information_schema.tables
        WHERE clustering_columns IS NOT NULL
    """)


# --------------------------------------------------------------------
# Table Size Monitoring
# --------------------------------------------------------------------

@dlt.table(
    name="table_storage_metrics",
    comment="Storage metrics for monitored tables"
)
def table_storage_metrics():

    inventory = spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name
        FROM system.information_schema.tables
        WHERE table_type='MANAGED'
    """)

    return (
        inventory
        .withColumn("observation_time", current_timestamp())
    )


# --------------------------------------------------------------------
# Candidate Tables for Liquid Clustering
# --------------------------------------------------------------------

@dlt.table(
    name="clustering_candidates",
    comment="Tables potentially suitable for liquid clustering"
)
def clustering_candidates():

    return spark.sql("""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            data_source_format,
            current_timestamp() AS recommendation_time
        FROM system.information_schema.tables
        WHERE data_source_format='DELTA'
    """)


