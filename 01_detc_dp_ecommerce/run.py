from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("membership_processing") \
    .getOrCreate()

spark.version

def to_date_(col, formats=("yyyy/MM/dd","yyyy-MM-dd","MM/dd/yyyy", "MM-dd-yyyy","dd-MM-yyyy")):
    return coalesce(*[to_date(col, f) for f in formats])


url="01_detc_dp_ecommerce/datafiles/raw/applications_dataset_1.csv"
_df = spark.read.csv(url, header=True)
_df.show()

df = _df.withColumn('increasing_id', f.monotonically_increasing_id())
df = df.withColumn('row_id', f.row_number().over(Window.orderBy('increasing_id')))
df = df.drop('increasing_id')

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

df_with_dob = df_validate_mobile_number.withColumn("dob", to_date_("date_of_birth"))
# df2.filter(col("dob").isNull()).show(1000)
# for col in df2.dtypes:
#     print(col[0]+" , "+col[1])
df_validate_mobile_number.show(10)
df_validate_dob = df_with_dob.withColumn("badRecord" \
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
df_validate_dob.show(10, False)


def current_local_date():
    return f.from_utc_timestamp(f.current_timestamp(), 'Asia/Singapore').cast('date')

# df3 = df_validate_dob.withColumn('age'
#                                  ,f.months_between(current_local_date(), f.col('dob')) / 12).cast('int')

# df2 = df_validate_dob.withColumn('age', (f.months_between(current_local_date(), f.col('dob')) / 12).cast('int'))   \
#             .withColumn('above_18'
#                                             ,f.when((f.months_between(current_local_date(), f.col('dob')) / 12).cast('int')>18,True)
#                                             .otherwise(False)
#                                         )         
# df2.show()                               

df_validate_age = df_validate_dob.withColumn('above_18'
                                                ,f.when((f.months_between(to_date(f.lit("2022-01-01"),"yyyy-MM-dd")  
                                                                        , f.col('dob')) / 12).cast('int')>18,True)
                                                .otherwise(False)
                                            )


expr = ".+@.+\.com|.biz"


# df_validate_email = df_validate_age.withColumn('valid_email'
#                                                ,f.when(col("email").rlike(expr),"valid")
#                                                .otherwise("invalid"))

# df_validate_email = df_validate_age.withColumn('valid_email'
#                                                 ,f.when(col("email").rlike(expr),"valid")
#                                                 .otherwise("invalid")) \
#                                         .withColumn('badRecordz'
#                                                ,f.when(((col("badRecord")==False) & (   col("email").rlike(expr) == True   )) == False
#                                                 ,True)
#                                             .otherwise(False)) 
#                                         # .withColumn("comments"
#                                         #             , f.when(col("badRecordz")==False, f.concat( col("comments"),f.lit(" | "),f.lit("email validation success" )))
#                                         #             .otherwise(f.concat ( col("comments"),f.lit(" | "), f.lit("email validation failed")))
#                                         #             )


df_validate_email = df_validate_age.withColumn('badRecord'
                                               ,f.when(((col("badRecord")==False) & (   col("email").rlike(expr) == True   )) == False
                                                ,True)
                                            .otherwise(False)) \
                                        .withColumn("comments"
                                                    , f.when( col("email").rlike(expr) == True  
                                                             , f.concat( col("comments"),f.lit(" | "),f.lit("email validation Success" )))
                                                    .otherwise(f.concat ( col("comments"),f.lit(" | "), f.lit("email validation Failed")))
                                                    )
    
    
# df_validate_email.filter((col("badRecord")==False) & (col("email").like('%.net')) ).show(1000,False)

df_validate_email.show(10,False)                                                

list_name_prefix =["Mr","Dr"]
df_split_name = df_validate_email.withColumn("name_without_prefix", f.when(( f.split(col("name"), "\.").getItem(0)).isin(list_name_prefix) ,
                                                                   f.trim(f.split(col("name"), "\.").getItem(1)))  
                                                            .otherwise(f.trim(f.split(col("name"), "\.").getItem(0)))
                                             )\
                                  .withColumn("first_name", f.split(col("name_without_prefix")," ").getItem(0))\
                                  .withColumn("last_name", f.split(col("name_without_prefix")," ").getItem(1))





# df_split_name.show(df_split_name.count())
df_split_name.show(10)

df_final = df_split_name.withColumn("membership_id"
                                    , f.when((col("badRecords")==False) & (col("above_18")==True) == True,
                                           f.concat( col("last_name"), f.sha2( f.date_format(col("dob"),"YYYYMMDD" )   ) ))
                                        .otherwise("")
                                            
                                    )

