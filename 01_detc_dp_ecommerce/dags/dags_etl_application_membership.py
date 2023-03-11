import os
import pandas as pd
from datetime import datetime
from airflow.models import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator
from airflow.models import Variable