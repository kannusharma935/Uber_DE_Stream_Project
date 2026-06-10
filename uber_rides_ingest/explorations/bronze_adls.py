# Databricks notebook source
import pandas as pd
files=[
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
# {"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}]


for file in files:
    url=f"https://uberprojectdevdatalake.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-06-07T18:47:06Z&se=2026-06-10T03:02:06Z&spr=https&sv=2026-02-06&sr=c&sig=GoP5NvjKojwnQmEfPLwGwTohSvye1UmFyHMRuK1ss5o%3D"

    df=pd.read_json(url)
    df_spark=spark.createDataFrame(df)
    
    #Writing data to bronze layer
    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
            .saveAsTable(f"uber.bronze.{file['file']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.rides_raw