# dm-spark-lab

The file structure of our project : 
1. data : Contains the data to use when running the code in the notebooks or scripts. Currently emptied in order to save space.
2. figures : Store the generated figures.
3. notebooks: Contains the notebooks with answers to questions about the dataset.
4. performance_scripts: Contains the performance scripts to be run on a local machine.
5. performance_scripts_gcp: Contains the performance scripts to be run on a cluster.

## Notes 
- In order to reduce unnecessary interaction between some data and avoid confusion we splitted the Data Analysis  into separate notebooks.

- You can find answers to questions regarding machines and their usage in the file Machine_event_Analysis.ipynb and answers to questions regarding tasks, jobs and their characteristics in the file  Task_Job_events_Analysis.ipynb. 


- Please note that if you want to execute the code yourself the Path variables leading to data location should be change to match the data location on your machine.

- Due to special usage (extending the work,  of the question *``Are the tasks that request the more resources the one that consume the more resources?``*), we separated the solution into scripts because of kernal crushes if we run them in jupyter notebook cells.
