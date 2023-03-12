from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col , coalesce, to_date
from pyspark.sql.types import StructType 
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import datetime 
import os
import shutil
import glob

class etl:

    raw_path = "/Users/santhoshjanakiraman/airflow/dags/datafiles/raw/"
    stage_path = "/Users/santhoshjanakiraman/airflow/dags/datafiles/stage"
    input_path = "/Users/santhoshjanakiraman/airflow/dags/datafiles/input"


    spark = SparkSession \
                    .builder \
                    .appName("membership_processing") \
                    .getOrCreate()
    emp_RDD = spark.sparkContext.emptyRDD()
    columns = StructType([])
    df_processing = spark.createDataFrame(data = emp_RDD,schema = columns)
    
    raw_folder_path = "/Users/santhoshjanakiraman/airflow/dags/datafiles/raw"
    stage_folder_path = "/Users/santhoshjanakiraman/airflow/dags/datafiles/stage"
        
    def to_date_(col, formats=("yyyy/MM/dd","yyyy-MM-dd","MM/dd/yyyy", "MM-dd-yyyy","dd-MM-yyyy")):
        return coalesce(*[to_date(col, f) for f in formats])

    
    def load_raw_data(raw_path, _run_id) :
        print("raw_path: " +raw_path)
        # run_id = datetime.datetime.now().strftime('%Y%m%d_%H')
        raw_path = raw_path+"/"+_run_id
        print("raw_path: " +raw_path)
        print("stage 1 - Load Raw Data")
        try:
            # raw_folder_path="/Users/santhoshjanakiraman/airflow/dags/datafiles/raw"        
            etl.df_processing = etl.spark.read.format("csv").option("header","true").load(raw_path).withColumn("filename",f.input_file_name())
            print("- data files from the path "+raw_path+" were successfully loaded to dataframe")
        except Exception as e:
            print("- exception:")

        
    def validate_mobile_number(df):
        print("stage 2 - Validate Mobile Niumber")
        etl.df_processing = df.withColumn("badRecord" 
                                , f.when(f.length                                           
                                            (f.regexp_replace                               
                                                    ( f.trim                                
                                                        (f.col("mobile_no")),"\\s+", ""     
                                                    )                                       
                                                ) == 8, False                               
                                            ).otherwise(True)) \
                        .withColumn("comments"
                                    , (f.when(f.col("badRecord")==True
                                                , "mobile_no validation failed")
                                        ).otherwise( "mobile_no validation Success"))\
                                                            

    # def validate_dob(df):
    #     etl.df_processing = df.withColumn("dob", etl.to_date_("date_of_birth"))

    def validate_dob(df):
        etl.df_processing = df.withColumn("dob", etl.to_date_("date_of_birth")) \
                                .withColumn("badRecord" \
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


    

    def validate_email(df, regex_fmt_email: str):
        # expr = ".+@.+\.com|.biz"
        etl.df_processing = df.withColumn('badRecord'
                                            ,f.when(((col("badRecord")==False) & (col("email").rlike(regex_fmt_email) == True   )) == False
                                                ,True)
                                            .otherwise(False)) \
                                .withColumn("comments"
                                            , f.when( col("email").rlike(regex_fmt_email) == True  
                                                    , f.concat( col("comments"),f.lit(" | "),f.lit("email validation Success" )))
                                            .otherwise(f.concat ( col("comments"),f.lit(" | "), f.lit("email validation Failed")))
                                            )


    def current_local_date():
        return f.from_utc_timestamp(f.current_timestamp(), 'Asia/Singapore').cast('date')

    def calculate_age(df):
        etl.df_processing = df.withColumn('above_18'
                                        ,f.when((f.months_between(to_date(f.lit("2022-01-01"),"yyyy-MM-dd")  
                                                                , f.col('dob')) / 12).cast('int')>18,True)
                                        .otherwise(False)
                                    )

    def split_first_last_name(df):  
        list_name_prefix =["Mr","Dr"]
        etl.df_processing = df.withColumn("name_without_prefix", f.when(( f.split(col("name"), "\.").getItem(0)).isin(list_name_prefix) ,
                                                                      f.trim(f.split(col("name"), "\.").getItem(1)))  
                                                                .otherwise(f.trim(f.split(col("name"), "\.").getItem(0))))\
                            .withColumn("first_name", f.split(col("name_without_prefix")," ").getItem(0))\
                            .withColumn("last_name", f.split(col("name_without_prefix")," ").getItem(1))


    def generate_membership_id(df):
        etl.df_processing = df.withColumn("membership_id"
                                    ,  f.when((col("badRecord")==False) & (col("above_18")==True) == True,
                                                f.concat(col("last_name"), f.lit("_") , f.substring(f.sha2( f.date_format(col("dob"),"yyyyMMdd" ) ,256) ,0,5))) 
                                        .otherwise(""))

    @staticmethod
    def process_application_membership(_run_id):
        print("rund id --------------------------------"+ _run_id)
        etl.load_raw_data(etl.raw_path, _run_id)
        etl.validate_mobile_number(etl.df_processing)
        etl.validate_dob(etl.df_processing)
        etl.validate_email(etl.df_processing,".+@.+\.com|.biz")
        etl.calculate_age(etl.df_processing)
        etl.split_first_last_name(etl.df_processing)
        etl.generate_membership_id(etl.df_processing)
        return etl.df_processing

    
    def delete_files(delete_path):
        for root, dirs, files in os.walk(delete_path):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))
                        
    
    def copy_files_and_folders(src, dest, _run_id):
        # runiId = date.now()
        # run_id = datetime.datetime.now().strftime('%Y%m%d_%H')
        print("runid: "+str(_run_id))
        dest = dest+"/"+_run_id
        print("newpath: "+str(dest))

        if not os.path.exists(dest):
            os.makedirs(dest)
        else:
            etl.delete_files(dest)

        for item in os.listdir(src):
            print("file: "+str(item))
            file_path = os.path.join(src, item)
            print("file_path: "+str(file_path))

            # if item is a file, copy it
            if os.path.isfile(file_path):
                shutil.copy(file_path, dest)

            # else if item is a folder, recurse 
            elif os.path.isdir(file_path):
                new_dest = os.path.join(dest, item)
                os.mkdir(new_dest)
                etl.recursive_copy(file_path, new_dest)

    @staticmethod
    def move_files_to_raw(_run_id):
        print("rund id --------------------------------"+ _run_id)
        etl.copy_files_and_folders(etl.input_path,etl.raw_path, _run_id)
        etl.delete_files(etl.input_path)
        
# etl.load_raw_data()
# etl.validate_mobile_number(etl.df_processing)
# etl.validate_dob(etl.df_processing)
# etl.validate_email(etl.df_processing,".+@.+\.com|.biz")
# etl.calculate_age(etl.df_processing)
# etl.split_first_last_name(etl.df_processing)
# etl.generate_membership_id(etl.df_processing)
# etl.df_processing.show()


# etl.copy_files_and_folders(input_path,raw_path)




# etl.delete_files(input_path)