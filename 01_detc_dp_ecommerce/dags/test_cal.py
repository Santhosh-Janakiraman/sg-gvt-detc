import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from etl_application_membership import etl

# def dp_exec():
#     obj = etl()
#     df=obj.start()
#     df.show()

# dp_exec()

# obj = etl()
# df= obj.start()
# df.show()

def dp_process_application_membership():
    e = etl()
    df = e.process_application_membership()
    df.show()

dp_process_application_membership()