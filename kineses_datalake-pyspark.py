{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "data_schema = spark.read.format('json').load('s3//s3-datalake-raw-teste/order-created').schema \n",
    "\n",
    "df = spark.readStream.format('json').schema(data_schema).load('s3//s3-datalake-raw-teste/order-created')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as F \n",
    "\n",
    "df = df.withColumn('created_date',  F.to_date(F.col('created_at')))\n",
    "\n",
    "df.writeStream.partitionBy('created_date').format('parquet').option(\"checkpointLocation\", 'dbfs:/order-created-staged-checkpoint').start('s3://s3-datalake-stage-teste/order-created')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "spark.conf.set(\"spark.sql.sources.partitionOverwriteMode\", \"dynamic\")\n",
    "\n",
    "agregated_df = df.groupBy(['created_date', 'product_name']).sum('quantity')\n",
    "agregated_df = agregated_df.withColumnRenamed('sum(quantity)', 'total_quantity')\n",
    "\n",
    "def _overwrite_partition(microbatch, epoch_id):\n",
    "    microbatch.write.partitionBy('created_date').mode('overwrite').format('parquet').save(\"s3://s3-datalake-curated-testeInfo/order-created\")\n",
    "\n",
    "agregated_df.writeStream.ouputMode('update').option('checkpointLocation', 'dbfs:/order-created-curated-checkpoint').foreachBeatch(_overwrite_partition).start()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "display(spark.read.format('parquet').load('s3://s3-datalake-curated-teste/order-created'))"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
