from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import col, expr

SOURCE_TABLE = "/Volumes/workspace/default/employee_data/lab1/source"

# Common data-quality condition
VALID_CONDITION = """
    customer_id IS NOT NULL
    AND customer_name IS NOT NULL
    AND TRIM(customer_name) <> ''
    AND city IS NOT NULL
    AND TRIM(city) <> ''
    AND sequence_num IS NOT NULL
"""

# 1. Valid customer events
@dp.table(name="customer_events_valid_sdp")
@dp.expect("valid_customer_data", VALID_CONDITION)
def customer_events_valid_sdp():
    return (
        spark.readStream
        .format("delta")
        .load(SOURCE_TABLE)
        .filter(VALID_CONDITION)
    )


# 2. Invalid records: quarantine table
@dp.table(name="customer_events_quarantine_sdp")
def customer_events_quarantine_sdp():
    return (
        spark.readStream
        .format("delta")
        .load(SOURCE_TABLE)
        .filter(f"NOT ({VALID_CONDITION})")
        .withColumn(
            "quarantine_reason",
            F.lit("Missing or empty customer field")
        )
    )




# Create the SCD Type 2 target table
dp.create_streaming_table(
    name="customer_history_scd2",
    comment="Customer history maintained using AUTO CDC SCD Type 2"
)

# Apply customer inserts, updates and deletes
dp.create_auto_cdc_flow(
    target="customer_history_scd2",
    source="customer_events_valid_sdp",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2
)


# Create the SCD Type 2 target
dp.create_streaming_table(
    name="customer_history_generated",
    comment="Generated customer history using AUTO CDC SCD Type 2"
)

# Apply changes in sequence
dp.create_auto_cdc_flow(
    target="customer_history_generated",
    source="customer_events_generated",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2
)