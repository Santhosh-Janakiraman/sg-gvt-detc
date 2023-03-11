from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("membership_processing") \
    .getOrCreate()

# spark.version

def to_date_(col, formats=("yyyy/MM/dd","yyyy-MM-dd","MM/dd/yyyy", "MM-dd-yyyy","dd-MM-yyyy")):
    return coalesce(*[to_date(col, f) for f in formats])

# from os import listdir
# from os.path import isfile, join
# raw_dir = "01_detc_dp_ecommerce/datafiles/raw/"
# raw_files = [f for f in listdir(dir_raw) if isfile(join(dir_raw, f))]

# for file in raw_files:
#     _df = spark.read.format("csv").option("header","true").load("01_detc_dp_ecommerce/datafiles/raw/"+ file).withColumn()
#     _df = _df.withColumn("filename",f.input_file_name)
# print(str(raw_files))
# exit


# url="01_detc_dp_ecommerce/datafiles/raw/applications_dataset_1.csv" 
# df = spark.read.csv(url, header=True)
df = spark.read.format("csv").option("header","true").load("01_detc_dp_ecommerce/datafiles/raw/").withColumn("filename",f.input_file_name())
df.show() 
print("count:"+ str(df.count()))
# file 1- 1999


# df = _df.withColumn('increasing_id', f.monotonically_increasing_id())
# df = df.withColumn('row_id', f.row_number().over(Window.orderBy('increasing_id')))
# df = df.drop('increasing_id')


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



#02-04-2011|  4250793|     true|mobile number val...|2011-02-04
#assumption - month always starts first ???

df_with_dob = df_validate_mobile_number.withColumn("dob", to_date_("date_of_birth"))

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


df_validate_age = df_validate_dob.withColumn('above_18'
                                                ,f.when((f.months_between(to_date(f.lit("2022-01-01"),"yyyy-MM-dd")  
                                                                        , f.col('dob')) / 12).cast('int')>18,True)
                                                .otherwise(False)
                                            )


expr = ".+@.+\.com|.biz"




df_validate_email = df_validate_age.withColumn('badRecord'
                                               ,f.when(((col("badRecord")==False) & (   col("email").rlike(expr) == True   )) == False
                                                ,True)
                                            .otherwise(False)) \
                                        .withColumn("comments"
                                                    , f.when( col("email").rlike(expr) == True  
                                                             , f.concat( col("comments"),f.lit(" | "),f.lit("email validation Success" )))
                                                    .otherwise(f.concat ( col("comments"),f.lit(" | "), f.lit("email validation Failed")))
                                                    )
    
    

df_validate_email.show(10,False)                                                

list_name_prefix =["Mr","Dr"]
df_split_name = df_validate_email.withColumn("name_without_prefix", f.when(( f.split(col("name"), "\.").getItem(0)).isin(list_name_prefix) ,
                                                                   f.trim(f.split(col("name"), "\.").getItem(1)))  
                                                            .otherwise(f.trim(f.split(col("name"), "\.").getItem(0)))
                                             )\
                                  .withColumn("first_name", f.split(col("name_without_prefix")," ").getItem(0))\
                                  .withColumn("last_name", f.split(col("name_without_prefix")," ").getItem(1))





df_split_name.show(10)

df_final = df_split_name.withColumn("membership_id"
                                    ,  f.when((col("badRecord")==False) & (col("above_18")==True) == True,
                                               f.concat(col("last_name"), f.lit("_") , f.substring(f.sha2( f.date_format(col("dob"),"yyyyMMdd" ) ,256) ,0,5))
                                       ) 
                                        .otherwise("")
                                    )

df_final.filter(col("badRecord")==False).show()

