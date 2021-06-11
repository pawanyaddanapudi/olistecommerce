from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.insert(0, "/opt/airflow/olistecommerce/")
from jobs.raw_load_job1 import *
from jobs.customers_mart import *
from jobs.geographics_mart import *
from jobs.reviews_mart import *



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
    start_date=datetime(2021,6,11,5,55,00),
    tags=['python', 'pipeline','olist'],
) as dag:

    job1_raw = PythonOperator(
        task_id='RawDataLoad',
        python_callable=raw_data_job1_inc
    )

    job2_table1 = PythonOperator(
        task_id='CustSalesLoad1',
        # bash_command='python /opt/airflow/script2.py'
        python_callable=job2_table1_inc
    )

    job3_table2 = PythonOperator(
        task_id='CustSalesLoad2',
        # bash_command='python /opt/airflow/script3.py'
        python_callable=job3_table2_inc
    )

    job4_table3 = PythonOperator(
        task_id='CustSalesLoad3',
        # bash_command='python /opt/airflow/script3.py'
        python_callable=job4_table3_inc
    )

    job5_table4 = PythonOperator(
        task_id='CustSalesLoad4',
        # bash_command='python /opt/airflow/script3.py'
        python_callable=job5_table4_inc
    )

    job6_geo = PythonOperator(
        task_id='GeoLoad',
        # bash_command='python /opt/airflow/script3.py'
        python_callable=job6_table1_inc
    )

    job7_review = PythonOperator(
        task_id='ReviewLoad',
        # bash_command='python /opt/airflow/script3.py'
        python_callable=job7_table1_inc
    )

    job1_raw >> [job2_table1, job6_geo, job7_review]
    job2_table1 >> [job3_table2, job4_table3, job5_table4]
