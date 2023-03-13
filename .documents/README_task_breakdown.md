# detc-000 [Analysis and Planning]

# DETC-101 [Prerequisites]
    - DOD:
        - Docker setup 
          - install docker in the machine youare executing 
        - Airflow setup 
          - install airflow in your local machine
        - Create contianer image to run spark code
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
    - Design 1 : 
      - DoD:
        -  Design database layer to handle request by below teams : 
           - Logistics: 
               - Get the sales details (in particular the weight of the total items bought)
               - Update the table for completed transactions
           - Analytics:
               - Perform analysis on the sales and membership status
               - Should not be able to perform updates on any tables
           - Sales:
               - Update databse with new items
               - Remove old items from database

    - Design 2 : 
      - DoD:
        - produce a system architecture diagram (Visio, PowerPoint, draw.io) depicting the end-to-end flow  for 
          -   Design data infrastructure on the cloud for a company whose main business is in processing images.The use cases to be considered are
              -   web application which allows users to upload images to the cloud using an API. 
              -   web application which hosts a Kafka stream that uploads images to the same cloud environment (Note: This Kafka stream has to be managed by the company's engineers)
              -   Business Intelligence resource where the company's analysts can access and perform analytical computation on the data store
              -   Other Considerations
                  - Securing access to the environment and its resources as the company expands
                  - Security of data at rest and in transit
                  - Scaling to meet user demand while keeping costs low
                  - Maintainance of the environment and assets (including processing scripts)
            - Design consdierations
                - Managability
                - Scalability
                - Secure
                - High Availability
                - Elastic
                - Fault Tolerant and Disaster Recovery
                - Efficient
                - Low Latency
                - Least Privilege

 # DETC-104 [Task 4 - Charts & API] 
    - DoD: Display a graph to show the number cases in Singapore over time using the APIs from https://covid19api.com/

 # DETC-104 [Task 5 - Machine Learning] 
    - Not attempted