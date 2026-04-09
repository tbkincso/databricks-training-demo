# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental batch ingestion from S3 with Auto Loader
# MAGIC
# MAGIC This notebook shows how to incrementally ingest CSV files from an S3 bucket using Auto Loader, with _schema evolution enabled_.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Connect to your S3 bucket, by creating a storage credential and an external location for S3 from the Databricks Catalog Explorer (follow the steps from [here](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual)).
# MAGIC - Create a `databricks_training` catalog with a `demo` schema in your Databricks workspace.
# MAGIC - Create the `incremental_ingestion_demo` folder on S3 and upload the first CSV file from `sample-data/incremental_ingestion_data`.
# MAGIC
# MAGIC ### How to try out this demo:
# MAGIC - Run the notebook cell to ingest current files from S3 and check the `databricks_training.demo.incremental_ingestion_data` table in your Databricks workspace.
# MAGIC - Upload a new CSV to the `incremental_ingestion_demo` folder in S3 (from `sample-data/incremental_ingestion_data`).
# MAGIC - Re-run the same cell to ingest new files (because of checkpointing, only the new files are processed) and check that new data appears in your table. (Because of schema evolution, starting from `demo_03`, a new `temperature` column should appear in your delta table.)
# MAGIC
# MAGIC Alternatively, you can schedule a job to run this notebook, for example hourly, to ingest newly appearing data in the S3 bucket. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

# Define the external location for AWS S3
external_location_name = "s3://szk-databricks-training-demo-s3/"

# Define the schema for the CSV files
# schema = StructType([
#     StructField("timestamp", TimestampType(), True),
#     StructField("value", IntegerType(), True)
# ])

# Define the schema location used for schema evolution
schema_location = f"{external_location_name}/schema/incremental_ingestion_demo"

# Use Auto Loader to incrementally ingest CSV files from the external location
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.schemaLocation", schema_location) # enable schema evolution instead of scpecifying the schema
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # enable schema evolution
      # .schema(schema)
      .load(f"{external_location_name}/incremental_ingestion_demo"))

# Define the checkpoint location for incremental batch ingestion
checkpoint_location = f"{external_location_name}/checkpoints/incremental_ingestion_demo"

# Write the ingested data to a Delta table in append mode, processing all available files and then stopping
query = (df.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_location)
         .option("mergeSchema", "true") # enable accepting schema changes on the Delta Table as well
         .trigger(availableNow=True) # process all files currently available in S3, then stop the stream
         .table("databricks_training.demo.incremental_ingestion_data"))

query.awaitTermination()

# COMMAND ----------

spark.read.table("databricks_training.demo.incremental_ingestion_data").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continuous streaming
# MAGIC
# MAGIC To run streaming ingestion instead of incremental batch ingestion, you need to change:
# MAGIC
# MAGIC `.trigger(availableNow=True)`
# MAGIC
# MAGIC to 
# MAGIC  
# MAGIC `.trigger(processingTime="5 seconds")`
# MAGIC
# MAGIC or simply remove the trigger, creating a long-running streaming query.
# MAGIC
# MAGIC _Note: This only works in the Databricks Enterprise version, Free Edition does not support it._
