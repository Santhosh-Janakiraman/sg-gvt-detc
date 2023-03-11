from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

@dag(start_date=datetime(2023, 3, 11), schedule_interval='@daily', catchup=False)
def docker_dag():

    @task()
    def t1():
        pass

    t2 = DockerOperator(
         task_id='docker-run-test'
        ,image='dp-executor:latest'
        ,command='python3 run.py'
        ,docker_url='unix://var/run/docker.sock'
        ,network_mode='bridge'
        
    )
        
    t1() >>  t2

dag = docker_dag()