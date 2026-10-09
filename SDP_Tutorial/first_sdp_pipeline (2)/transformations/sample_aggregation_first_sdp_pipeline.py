'''from pyspark import pipelines as dp #dp means declartive pipeline
from pyspark.sql.functions import col,count,count_if
from utilities import utils #if we have anything is within in utilies.py then we import from there.




@dp.table()
def sample_aggregation_first_sdp_pipeline():
    return(
        spark.read.table("samples.wanderbricks.users")
        .withColumn("valid_email",utils.is_valid_email(col("email")))
        .groupBy(col("user_type"))
        .agg(
            count("user_id").alias("total_count"),
            count_if("valid_email").alias("count_valid_emails")
        )
    )'''