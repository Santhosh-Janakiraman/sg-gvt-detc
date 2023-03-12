# detc-000 [Analysis and Planning]

# DETC-101 [Prerequisites]
    - DOD:
        - Docker setup 
          - install docker in the machine youare executing 
        - airflow setup 
          - install airflow in your local machine
        - create contianer image to run spark code
          - In ideal world, the spark code should be executed from an docker image, while running on kubernetes. so create a Docker File for that purpose
          - This file will not be used in this implementation


# DETC-102 [Tasl 1 - Data Pipelines ] & [Task2 - Databases ]
    - DOD:
        - create pipeline task in spark (pyspark) to validate the given records and generate final datasets
          - Dataset 1 : Valid memerbship list, as per the details given in requiremtn
          - Dataset 2 : error records. The list includes the error detils in the column comments
        - create dags 
          - Create a dag to excure below task
            - Copy sources file to Raw Folder
              - Copy the file from the input folder (assuming this is the source location that will receive the files ) to raw
              - In raw folder create a subfolder with runId (runId is the date time in "yyyyMMdd_HH" format)
              - Paste the copied files from input folder to raw/<runId> folder
              - Remove the files in Input folder, to avoid processing the files again in next run
            - Validate and Transform input data
              - Load files in raw folder to a dataframe
              - Execute below validations
                - Validate Mobile number
                - Validate dob
                - Validate email address
              - Fucntional Requirements
                - Calculate Age
                - Split full name in to first and last name
              - Prepare final data set
              - Write the result dataframt to stage folder
      - Note: if there is any aggregations or calculations, then those will be perormed and moved to processed folder. As Ther are no such requirement, this folder will be empty for now. 
                

 # DETC-103 [Task 3 - System Design]
    - DoD:
      - 

 # DETC-104 [Task 4 - Charts & API] 
    - DoD:
      - Your team decided to design a dashboard to display the statistic of COVID19 cases. You are tasked to display one of the components of the dashboard which is to display a visualisation representation of number of COVID19 cases in Singapore over time.

Your team decided to use the public data from https://documenter.getpostman.com/view/10808728/SzS8rjbc#b07f97ba-24f4-4ebe-ad71-97fa35f3b683.

Display a graph to show the number cases in Singapore over time using the APIs from https://covid19api.com/.

Note: please submit screenshots of the dashboard