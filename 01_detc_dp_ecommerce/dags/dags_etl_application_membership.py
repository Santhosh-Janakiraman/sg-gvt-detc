import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from etl_application_membership import etl



def dp_process_application_membership():
    e = etl()
    df = e.process_application_membership()
    df.show()

def dp_move_files_to_raw():
    e = etl()
    e.move_files_to_raw()
    

with DAG(
    dag_id='dag_etl_appliction_membership'
    ,schedule_interval='* * * * *'
    ,start_date=datetime(year=2022, month=3, day=11)
    ,catchup=False
) as dag: 
    

    psstart = EmptyOperator(task_id='psstart')
    
    task_move_files_to_raw = PythonOperator(
                                            task_id='task_move_files_to_raw',
                                            python_callable=dp_move_files_to_raw
                                            )

    task_process_application_membership = PythonOperator(
                                                task_id='task_process_application_membership',
                                                python_callable=dp_process_application_membership
                                                )

    psend = EmptyOperator(task_id='psend')

    psstart >> task_move_files_to_raw >> task_process_application_membership >> psend
    # psstart >> task_move_files_to_raw >> psend

     