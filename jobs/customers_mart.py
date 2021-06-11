import pandas as pd
from sqlalchemy import create_engine
import psycopg2





def job2_table1_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    get_max_time_sql = 'select * from olist_staging.pipelinetimestamp'
    max_date = pd.read_sql(get_max_time_sql, pg_engine)
    order_max_time = max_date['earlier_max_timestamp'][0]
    raw_data_sql = "select * from olist_staging.orskl_staging_rawdata where order_purchase_timestamp > '" +str(order_max_time)+"'"
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    raw_data['yearweek'] = raw_data['order_purchase_timestamp'].apply(lambda x: str(x.year)+str("{:02}".format(x.weekofyear)))
    table1 = raw_data[['yearweek','seller_id','product_category_name_english','customer_id','order_item_id','price',
                       'freight_value']].groupby(['yearweek','seller_id','product_category_name_english','customer_id']).\
        agg({'order_item_id':'sum', 'price':'sum', 'freight_value':'sum'}).rename(columns={'order_item_id':'itemssold','price':'total_price'}).\
        reset_index()
    table1['total_margin'] = table1['total_price'] - table1['freight_value']
    table1.to_sql("orskl_olist_cust_sales_tab1", con=conn, schema='olist_cust_sales_mart', index=False, if_exists='append')


def job2_table1():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    raw_data_sql = 'select * from olist_staging.orskl_staging_rawdata'
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    raw_data['yearweek'] = raw_data['order_purchase_timestamp'].apply(lambda x: str(x.year)+str("{:02}".format(x.weekofyear)))
    table1 = raw_data[['yearweek','seller_id','product_category_name_english','customer_id','order_item_id','price',
                       'freight_value']].groupby(['yearweek','seller_id','product_category_name_english','customer_id']).\
        agg({'order_item_id':'sum', 'price':'sum', 'freight_value':'sum'}).rename(columns={'order_item_id':'itemssold','price':'total_price'}).\
        reset_index()
    table1['total_margin'] = table1['total_price'] - table1['freight_value']
    table1.to_sql("orskl_olist_cust_sales_tab1", con=conn, schema='olist_cust_sales_mart', index=False)

def job3_table2():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table2 = sales_data.groupby(['yearweek','seller_id']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table2.to_sql("orskl_olist_cust_sales_tab2", con=conn, schema='olist_cust_sales_mart', index=False)

def job3_table2_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table2 = sales_data.groupby(['yearweek','seller_id']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table2.to_sql("orskl_olist_cust_sales_tab2", con=conn, schema='olist_cust_sales_mart', index=False, if_exists='append')


def job4_table3():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table3 = sales_data.groupby(['yearweek','seller_id','product_category_name_english']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table3.to_sql("orskl_olist_cust_sales_tab3", con=conn, schema='olist_cust_sales_mart', index=False)

def job4_table3_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table3 = sales_data.groupby(['yearweek','seller_id','product_category_name_english']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table3.to_sql("orskl_olist_cust_sales_tab3", con=conn, schema='olist_cust_sales_mart', index=False, if_exists='append')

def job5_table4():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table4 = sales_data.groupby(['yearweek','customer_id']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table4.to_sql("orskl_olist_cust_sales_tab4", con=conn, schema='olist_cust_sales_mart', index=False)

def job5_table4_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    sales_data_sql = 'select * from olist_cust_sales_mart.orskl_olist_cust_sales_tab1'
    sales_data = pd.read_sql(sales_data_sql, pg_engine)
    table4 = sales_data.groupby(['yearweek','customer_id']).agg({'itemssold':'sum', 'total_price':'sum', 'total_margin':'sum'}).\
        reset_index()
    table4.to_sql("orskl_olist_cust_sales_tab4", con=conn, schema='olist_cust_sales_mart', index=False, if_exists='append')