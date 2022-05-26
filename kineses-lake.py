# Databricks notebook source
data_schema = spark.read.format('json').load('s3//s3-datalake').schema 

df = spark.readStream.format('json').schema(data_schema).load('s3//s3-datalake')

# COMMAND ----------

from pyspark.sql import functions as F 

df = df.withColumn('created_date',  F.to_date(F.col('created_at')))

df.writeStream.partitionBy('created_date').format('parquet').option("checkpointLocation", 'dbfs:/order-created-staged-checkpoint').start('s3://s3-datalake-stage')

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

agregated_df = df.groupBy(['created_date', 'product_name']).sum('quantity')
agregated_df = agregated_df.withColumnRenamed('sum(quantity)', 'total_quantity')

def _overwrite_partition(microbatch, epoch_id):
    microbatch.write.partitionBy('created_date').mode('overwrite').format('parquet').save("s3://s3")

agregated_df.writeStream.ouputMode('update').option('checkpointLocation', 'dbfs:/order-created-curated-checkpoint').foreachBeatch(_overwrite_partition).start()

# COMMAND ----------

display(spark.read.format('parquet').load('s3://s3-datalake-d'))
