import sys,timeit
from statistics import mean 
import dask.dataframe as dd


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
    
    #Read the data into a Dask DataFrame
    df_task_event = dd.read_csv(path_task_event, compression='gzip', names=["timestamp","missing_info","job_id","task_index_job","machine_id","event_type",
                               "username","scheduling_class","priority","cpu_request","memory_request",
                               "disk_request", "machine_restriction"])
    
     
    df_task_usage = dd.read_csv(path_task_usage, compression='gzip',names=["start_time","end_time","job_id","task_index_job","machine_id",
                                "cpu_rate","canonial_memory","assigned_memory","unmapped_page_cache",
                                "total_page_cache","max_memory_usage","io_time","local_disk_usage",
                                "max_disk_usage","max_io_time", "cycle_per_inst","memory_access","sample_portion", 
                                "agg_type","cpu_usage"])
    
    
    stop = timeit.default_timer() 
    times_read_data.append(stop - start)
    
    start = timeit.default_timer()
    #Selecting only the columns we need
    df_task_event = df_task_event[['job_id','task_index_job','cpu_request','disk_request','memory_request']]
    df_task_usage = df_task_usage[['job_id','task_index_job','cpu_usage','local_disk_usage','assigned_memory']]
    
    stop = timeit.default_timer()   
    times_select.append(stop - start)
    
    start = timeit.default_timer()
    
    #Deleting the entries with null CPU requested and 
    df_task_event = df_task_event[df_task_event['cpu_request'].notnull()]
    
    stop = timeit.default_timer() 
    times_simple_filter.append(stop - start)
    
    start = timeit.default_timer()
    
    joined_df = df_task_event.merge(df_task_usage, on=['job_id','task_index_job'])
       
    stop = timeit.default_timer() 
    times_join.append(stop - start)
    
    start = timeit.default_timer()
    less_ressources = joined_df[(0.1 * joined_df["cpu_request"] > joined_df["cpu_usage"]) & 
                       (0.1 * joined_df["memory_request"] > joined_df["assigned_memory"]) &
                       (0.1 * joined_df["disk_request"] > joined_df["local_disk_usage"])]
    
    stop = timeit.default_timer() 
    times_multiple_filter.append(stop - start)
    
    
    tasks_asking_for_less_resources_pourcentage = less_ressources.compute().count()[0] / joined_df.compute().count()[0] * 100
    print('The poucentage of tasks that ask for resources needed is {}'
      .format(tasks_asking_for_less_resources_pourcentage))
    
    global_stop = timeit.default_timer() 
    times_getting_result.append(global_stop - global_start)
    
dask_times = [mean(times_read_data),
               mean(times_select),
               mean(times_join),
               mean(times_simple_filter),
               mean(times_multiple_filter),
               mean(times_getting_result)]

print(dask_times)