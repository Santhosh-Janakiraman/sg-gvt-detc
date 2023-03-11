import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from airflow.models import Variable

def process_datetime(ti):
    return (str("sdfajsldkfjslkdfjalksdf"))
    # dt = ti.xcom_pull(task_ids=['get_datetime'])
    # if not dt:
    #     raise Exception('No datetime value')
    
    # dt = str(dt[0]).split()
    # return {
    #     'year': int(dt[-1])
    #     ,'month': int(dt[1])
    #     ,'day': int(dt[2])
    #     ,'time': int(dt[3])
    #     ,'day_of_montj': int(dt[0])
    # }


with DAG(
    dag_id='first_airflow_dag'
    ,schedule_interval='* * * * *'
    ,start_date=datetime(year=2022, month=3, day=11)
    ,catchup=False
) as dag: 
    
    # task_get_datetime = BashOperator(
    #     task_id='get_datetime',
    #     bash_command='date'
    # )

    # task_process_datetime = PythonOperator(
    #     task_id='process_datetime',
    #     python_callable=process_datetime
    # )
    
    docker_test_task = DockerOperator(
        task_id='docker_test_task',
        image='dp-executor:latest',
        api_version='auto',
        # auto_remove=True,
        # mount_tmp_dir=False,
        # container_name='dp-application-memebership',
        command='echo "this is a test message shown from within the container',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
        #test_adsfadsfadsfsd
    )