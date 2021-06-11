from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from jobs.raw_load_job1 import job1_table1
from jobs.customers_mart import *
from jobs.reviews_mart import *
import sys
sys.path.insert(0, "/home/myname/pythonfiles")



default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'OlistDataPipeline',
    default_args=default_args,
    start_date=datetime(2021,5,30,4,30,00),
    tags=['python', 'pipeline','olist'],
) as dag:

    t1 = PythonOperator(
        task_id = 'rawload',
        #bash_command='. /opt/airflow/script1.bash'
        python_callable=script1
    )

    t2 = PythonOperator(
        task_id='script2',
        #bash_command='python /opt/airflow/script2.py'
        python_callable=script2
    )

    t3 = PythonOperator(
        task_id='script3',
        #bash_command='python /opt/airflow/script3.py'
        python_callable=script3
    )

    [t1, t2] >> t3
