from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql import functions as f


spark = SparkSession \
    .builder \
    .appName("membership_processing") \
    .getOrCreate()

spark.version

def to_date_(col, formats=("yyyy/MM/dd","yyyy-MM-dd","MM/dd/yyyy", "MM-dd-yyyy","dd-MM-yyyy")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])


url="01_detc_dp_ecommerce/datafiles/raw/applications_dataset_1.csv"
df = spark.read.csv(url, header=True)
df.show()

# filter mobile number 
# df_validate_mobile_number = df.filter(length(col("mobile_no")) == 8).show()
df_validate_mobile_number = df.withColumn("badRecord" 
                                          , f.when(f.length                                     \
                                                    (f.regexp_replace                           \
                                                            ( f.trim                            \
                                                                (f.col("mobile_no")),"\\s+", "" \
                                                            )                                   \
                                                        ) == 8, False                           \
                                                    ).otherwise(True))                          \
                                          .withColumn("comments"
                                                      , (f.when(f.col("badRecord")==True, "mobile_no validation failed")
                                                         ).otherwise( "mobile_no validation Success"))

# df_validate_mobile_number.show(10, False)

# from pyspark.sql import functions as f
# df2 = df.withColumn("badRecord", f.when(f.to_date(f.col("SMIC"),"dd/MM/yyyy").isNotNull, False).otherwise(True))


#02-04-2011|  4250793|     true|mobile number val...|2011-02-04
#assumption - month always starts first ???

df2= df_validate_mobile_number.withColumn("dob", to_date_("date_of_birth"))
# df2.filter(col("dob").isNull()).show(1000)
# for col in df2.dtypes:
#     print(col[0]+" , "+col[1])
df_validate_mobile_number.show(10)
df3 = df2.withColumn("badRecord" \
                     ,f.when(((col("badRecord")==False) & (col("dob").isNotNull())) == False
                             ,True)
                        .otherwise(False)
                     )\
            .withColumn("comments"
                     ,f.when( col("dob").isNull()
                             ,f.concat( col("comments")
                                       ,f.lit(" | ")
                                       ,f.lit("date_of_birth validation failed")
                                        )
                            )
                        .otherwise
                            (f.concat(        
                                     col("comments")
                                    ,f.lit(" | ")
                                    ,f.lit("date value <") 
                                    ,col("date_of_birth")               
                                    ,f.lit("> formatted as <")            
                                    ,col("dob")                 
                                    ,f.lit(">"))))
df3.show(10, False)

# df3 = df_validate_mobile_number.withColumn("badRecord"
#                                            ,f.when(f.to_date(f.col)))
#validate_dates
# df_validate_date_format_yyyymmdd = df_validate_mobile_number.
                                                        






