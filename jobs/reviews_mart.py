import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def job7_table1():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    raw_data_sql = 'select * from olist_staging.orskl_staging_rawdata'
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    reviews_data = raw_data.groupby(['product_category_name_english']).\
        agg({'review_score':'mean', 'order_purchase_timestamp':'max'}).reset_index()
    reviews_data.to_sql('orskl_olist_reviews_tab1', con=conn, schema='olist_review_mart',index=False)


def job7_table1_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    get_max_time_sql = 'select * from olist_staging.pipelinetimestamp'
    max_date = pd.read_sql(get_max_time_sql, pg_engine)
    order_max_time = max_date['earlier_max_timestamp'][0]
    raw_data_sql = "select * from olist_staging.orskl_staging_rawdata where order_purchase_timestamp > '" + str(
        order_max_time) + "'"
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    reviews_data = raw_data.groupby(['product_category_name_english']).\
        agg({'review_score':'mean', 'order_purchase_timestamp':'max'}).reset_index()
    reviews_data.to_sql('orskl_olist_reviews_tab1', con=conn, schema='olist_review_mart',index=False, if_exists='append')


