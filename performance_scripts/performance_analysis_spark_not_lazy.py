import sys,timeit

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType, LongType
import pyspark.sql.functions as F
from statistics import mean


#Create a spark context object with all the local available CPUs
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")


#Create a spark session
spark = SparkSession.builder \
                .master("local") \
                .appName("tasks-events-analysis") \
                .getOrCreate()

# 1. Task Events DataFrame Structure
schema_task_events = StructType([
    StructField("timestamp", LongType(), True),
    StructField("missing_info", StringType(), True),
    StructField("job_id", LongType(), True),
    StructField("task_index_job", IntegerType(), True),
    StructField("machine_id", LongType(), True),
    StructField("event_type", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("scheduling_class", IntegerType(), True),
    StructField("priority", IntegerType(), True),
    StructField("cpu_request", FloatType(), True),
    StructField("memory_request", FloatType(), True),
    StructField("disk_request", FloatType(), True),
    StructField("machine_restriction", IntegerType(), True)])

# 2. Task Usage DataFrame Structure
schema_task_usage = StructType([
    StructField("start_time", LongType(), True),
    StructField("end_time", StringType(), True),
    StructField("job_id", LongType(), True),
    StructField("task_index_job", IntegerType(), True),
    StructField("machine_id", LongType(), True),
    StructField("cpu_rate", IntegerType(), True),
    StructField("canonial_memory", StringType(), True),
    StructField("assigned_memory", IntegerType(), True),
    StructField("unmapped_page_cache", IntegerType(), True),
    StructField("total_page_cache", FloatType(), True),
    StructField("max_memory_usage", FloatType(), True),
    StructField("io_time", FloatType(), True),
    StructField("local_disk_usage", FloatType(), True),
    StructField("max_disk_usage", FloatType(), True),
    StructField("max_io_time", FloatType(), True),
    StructField("cycle_per_inst", FloatType(), True),
    StructField("memory_access", FloatType(), True),
    StructField("sample_portion", FloatType(), True),
    StructField("agg_type", FloatType(), True),
    StructField("cpu_usage", FloatType(), True)])

#The path to the data file
path_task_event = "./data/task_events/*.csv.gz"

#The path to the data file
path_task_usage = "./data/task_usage/*.csv.gz"

times_read_data = []
times_select = []
times_join = []
times_simple_filter = []
times_multiple_filter = []
times_getting_result = []

for i in range(10):
    global_start = timeit.default_timer()
    
    start = timeit.default_timer()
    
    #Read the data into a SparkDataFrame
    df_task_event = spark.read.csv(path_task_event, header = False, schema = schema_task_events)

    #Read the data into a SparkDataFrame
    df_task_usage = spark.read.csv(path_task_usage, header = False, schema = schema_task_usage)
    
    df_task_event.count()
    df_task_usage.count()
    stop = timeit.default_timer() 
    times_read_data.append(stop - start)
    
    start = timeit.default_timer()
    #Selecting only the columns we need
    df_task_event = df_task_event.select('job_id','task_index_job','cpu_request','disk_request','memory_request')
    df_task_usage = df_task_usage.select('job_id','task_index_job','cpu_usage','local_disk_usage','assigned_memory')
    df_task_event.count()
    df_task_usage.count()
    stop = timeit.default_timer()   
    times_select.append(stop - start)
    
    start = timeit.default_timer()
    
    #Deleting the entries with null CPU requested and 
    df_task_event = df_task_event.where(F.col('cpu_request').isNotNull())
    df_task_event.count()
    stop = timeit.default_timer() 
    times_simple_filter.append(stop - start)
    
    start = timeit.default_timer()
    
    joined_df = df_task_event.join(df_task_usage, on = ['job_id','task_index_job'])
    joined_df.count()
    stop = timeit.default_timer() 
    times_join.append(stop - start)
    
    start = timeit.default_timer()
    less_ressources = joined_df.filter((0.1 * joined_df["cpu_request"] > joined_df["cpu_usage"] ) & 
                       (0.1 * joined_df["memory_request"] > joined_df["assigned_memory"]) &
                       (0.1 * joined_df["disk_request"] > joined_df["local_disk_usage"]))
    less_ressources.count()
    stop = timeit.default_timer() 
    times_multiple_filter.append(stop - start)
    
    
    tasks_asking_for_less_resources_pourcentage = less_ressources.count() / joined_df.count() * 100
    print('The poucentage of tasks that ask for resources needed is {}'
      .format(tasks_asking_for_less_resources_pourcentage))
    
    global_stop = timeit.default_timer() 
    times_getting_result.append(global_stop - global_start)
    
spark_lazy_times = [mean(times_read_data),
               mean(times_select),
               mean(times_join),
               mean(times_simple_filter),
               mean(times_multiple_filter),
               mean(times_getting_result)]
print(spark_lazy_times)