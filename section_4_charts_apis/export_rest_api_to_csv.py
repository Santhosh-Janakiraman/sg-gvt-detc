import requests
import json
from pyspark.sql import SparkSession
from typing import List
from pyspark.sql.types import StructType, StructField, StringType
from pandas.io.json import json_normalize
from pyspark.sql import functions as f
from pyspark.sql.functions import length, col , coalesce, to_date

def read():
    
    export_path = "Source_data/csv/"

    spark = SparkSession \
                    .builder \
                    .appName("membership_processing") \
                    .getOrCreate()
    
    end_point_url = "https://api.covid19api.com/country/singapore"

    response = requests.get(
                        url = end_point_url                        
                    )

    dict = json.loads(response.text)
    pandas_df = json_normalize(dict)  
    df = spark.createDataFrame(pandas_df) 
    # df.show()

    df = df.withColumn("MonthPeriod",f.substring(col("Date"),0,7))\
            .withColumn("YHearPeriod",f.substring(col("Date"),0,4))
    
    df.repartition(1).write.option("header","true").csv(path=export_path,mode="overwrite")
    df = df.filter(col("MonthPeriod")=='2022-07')
    df.show(df.count())
read()
    