from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql import functions as f


spark = SparkSession \
    .builder \
    .appName("membership_processing") \
    .getOrCreate()

spark.version

def to_date_(col, formats=("MM/dd/yyyy", "yyyy-MM-dd")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])


url="01_detc_dp_ecommerce/datafiles/raw/applications_dataset_1.csv"
df = spark.read.csv(url, header=True)
df.show()

# filter mobile number 
# df_validate_mobile_number = df.filter(length(col("mobile_no")) == 8).show()
df_validate_mobile_number = df.withColumn("badRecord" 
                                          , f.when(f.length(f.col("mobile_no")) == 8, False).otherwise(True))\
                                          .withColumn("comments", (f.when(f.col("badRecord")==True, "mobile number validation failed")).otherwise( "mobile number validation Success"))

df_validate_mobile_number.show(1000, False)

from pyspark.sql import functions as f
df2 = df.withColumn("badRecord", f.when(f.to_date(f.col("SMIC"),"dd/MM/yyyy").isNotNull, False).otherwise(True))


df3 = df_validate_mobile_number.withColumn("badRecord"
                                           ,f.when(f.to_date(f.col)))
#validate_dates
# df_validate_date_format_yyyymmdd = df_validate_mobile_number.
                                                        






