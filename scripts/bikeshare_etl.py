#Pyspark script to ingest austin bikeshare data
from pyspark.sql import SparkSession
from pyspark.sql import functions as spark_func
from datetime import timedelta
import argparse
import sys

#Getting arguments

parser = argparse.ArgumentParser(description="Pyspark script for daily load of bikeshare data")

parser.add_argument('-initial_load', action='store', default=None)
parser.add_argument('-manual_load', action='store', default=None)
parser.add_argument('-bucket', action='store', default=None)
parser.add_argument('-table', action='store', default=None)
parser.add_argument('-datalake_path', action='store', default=None)


args = parser.parse_args()

# Creating spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName(f'{args.table}') \
  .getOrCreate()



# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.

spark.conf.set('temporaryGcsBucket', args.bucket)

# Load data from BigQuery.

source_df = spark.read.format('bigquery') \
  .option('table', f'bigquery-public-data:austin_bikeshare.{args.table}') \
  .load()
source_df.createOrReplaceTempView('source_df')


# Helper functions to read/write data and state

# Writes the state of the current run
def state_writer(df,write_mode,bucket,table):
    df.write.parquet(f"gs://{bucket}/state/{table}",mode=write_mode)

# Reads the latest state of the table
def state_reader(bucket,table):
    try:
        state_path=f"gs://{bucket}/state/{table}"
        state_df = spark.read.parquet(state_path)
        return state_df.select(spark_func.max('state')).rdd.flatMap(lambda x: x).collect()[0]
    except Exception as e:
        print(e)
        sys.exit(f"********** \n Could not find state at {state_path}. If it is the first run, please set initial load date in the config file \n**********")

# Writes the current table load to the GCS. Setting partition_overwrite_mode to 'DYNAMIC' will overwrite specific partitions
def target_writer(df,bucket,datalake_path,table,write_mode,partition_overwrite_mode='STATIC'):
    df.write.option('maxRecordsPerFile',1000000).option("partitionOverwriteMode",partition_overwrite_mode).partitionBy("partition_date","partition_time").\
        parquet(f"gs://{bucket}/{datalake_path}/{table}",mode=write_mode)


# Checking if initial load flag is set to True. This will overwrite all existing data in the table location as well as the state
if args.initial_load=="True":
    df = spark.sql("SELECT *,date_format(start_time, 'yyyy-MM-dd') as partition_date, date_format(start_time,'HH') as partition_time from source_df where to_date(start_time) between '2024-01-01' and '2024-01-02'")
    df.cache()
    df.createOrReplaceTempView('write_df')
    state_df = spark.sql("select max(to_date(start_time)) as state, current_date() as etl_date, count(*) as count from write_df")
    target_writer(df,args.bucket,args.datalake_path,args.table,'overwrite')
    state_writer(state_df,"overwrite",args.bucket,args.table)

else:
    #If manual load date is specified, it will overwrite the load date partition
    if args.manual_load:
        current_state = args.manual_load  
        write_mode='overwrite'
        partition_overwrite_mode='DYNAMIC'
        
    else:
        # If manual load date is not specified, current state is read from the state path
        current_state = (state_reader(args.bucket,args.table) + timedelta(days=1)).strftime("%Y-%m-%d")
        write_mode='append'
        partition_overwrite_mode='STATIC'
    
    # Priting current state
    print("##########",current_state,type(current_state))
    df = spark.sql("select *,date_format(start_time, 'yyyy-MM-dd') as partition_date, date_format(start_time,'HH') as partition_time from source_df where to_date(start_time) = '{}'".format(current_state))
    df.cache()
    df.createOrReplaceTempView('write_df')
    state_df = spark.sql("select max(to_date(start_time)) as state, current_date() as etl_date, count(*) as count from write_df")
    target_writer(df,args.bucket,args.datalake_path,args.table,write_mode,partition_overwrite_mode)
    state_writer(state_df,"append",args.bucket,args.table)

    
state_df.show()
df.unpersist()



