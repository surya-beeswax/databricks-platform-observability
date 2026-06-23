import dlt
from pyspark.sql.functions import current_timestamp


@dlt.table(
    name="cluster_health"
)
def cluster_health():

    return (
        spark.sql("""
            SELECT
                current_timestamp() AS observation_time,
                current_user() AS user_name
        """)
    )