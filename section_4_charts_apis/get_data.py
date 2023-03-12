from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql.types import StructType 
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import datetime 
import os
import shutil
import glob


data_file = "/Users/santhoshjanakiraman/Personal/Projects/gvtech/sg-gvt-detc/sg-gvt-detc/04_detc_analytics/Source_data/data.json"
export_path = "/Users/santhoshjanakiraman/Personal/Projects/gvtech/sg-gvt-detc/sg-gvt-detc/04_detc_analytics/Source_data/csv"

spark = SparkSession \
                    .builder \
                    .appName("membership_processing") \
                    .getOrCreate()

df = spark.read.option("multiline","true").json(data_file)
df.show()
df.repartition(1).write.option("header","true").csv(path=export_path,mode="overwrite")

            