# Go get the data of 9 tables from postgresql into the python kernel as dataframes
# Join all the dataframes as needed
# Dump the final dataframe into a table in the olist_staging schema

import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def initial_load():
    pg_engine = psycopg2.connect(dbname='airflow', host='localhost', port=5032, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@localhost:5032/airflow')

    customers_sql = 'select * from olist.orskl_customers'
    customers = pd.read_sql(customers_sql, pg_engine)

    gelocations_sql = 'select * from olist.orskl_geolocations'
    geolocations = pd.read_sql(gelocations_sql, pg_engine)

    order_items_sql = 'select * from olist.orskl_order_items'
    order_items = pd.read_sql(order_items_sql, pg_engine)

    order_payments_sql ='select * from olist.orskl_order_payments'
    order_payments = pd.read_sql(order_payments_sql, pg_engine)

    order_reviews_sql = 'select * from olist.orskl_order_reviews'
    order_reviews = pd.read_sql(order_reviews_sql, pg_engine)

    orders_sql = 'select * from olist.orskl_orders'
    orders = pd.read_sql(orders_sql, pg_engine)

    products_sql = 'select * from olist.orskl_products'
    products = pd.read_sql(products_sql, pg_engine)

    sellers_sql = 'select * from olist.orskl_sellers'
    sellers = pd.read_sql(sellers_sql, pg_engine)

    translation_sql = 'select * from olist.orskl_translation'
    translation = pd.read_sql(translation_sql, pg_engine)

    olist_final = orders.join(order_items.set_index('order_id'), on='order_id', how='left').\
        join(products.set_index('product_id'), on='product_id', how='left').\
        join(translation.set_index('product_category_name'), on='product_category_name', how='left').\
        join(order_payments.set_index('order_id'), on='order_id', how='left').\
        join(customers.set_index('customer_id'), on='customer_id', how='left').\
        join(sellers.set_index('seller_id'), on='seller_id', how='left')

    geolocations_unique = geolocations[['geolocation_zip_code_prefix','geolocation_lat','geolocation_lng']].\
        groupby('geolocation_zip_code_prefix').agg({'geolocation_lat': 'min', 'geolocation_lng':'min'}).reset_index()

    olist_final = orders.join(order_items.set_index('order_id'), on='order_id', how='left').\
        join(products.set_index('product_id'), on='product_id', how='left').\
        join(translation.set_index('product_category_name'), on='product_category_name', how='left').\
        join(order_payments.set_index('order_id'), on='order_id', how='left').\
        join(customers.set_index('customer_id'), on='customer_id', how='left').\
        join(sellers.set_index('seller_id'), on='seller_id', how='left').\
        merge(geolocations_unique, how='left', left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix',
              suffixes=("_x","_y")).\
        merge(geolocations_unique, how='left', left_on='seller_zip_code_prefix', right_on='geolocation_zip_code_prefix').\
        merge(order_reviews, how='left', on='order_id')

    olist_final.to_sql("orskl_staging_rawdata", con=conn, schema='olist_staging', index=False)

def raw_data_job1_inc():
    # pg_engine = psycopg2.connect(dbname='airflow', host='localhost', port=5032, user='airflow', password='airflow')
    # conn = create_engine('postgresql://airflow:airflow@localhost:5032/airflow')
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')

    customers_sql = 'select * from olist.orskl_customers'
    customers = pd.read_sql(customers_sql, pg_engine)

    gelocations_sql = 'select * from olist.orskl_geolocations'
    geolocations = pd.read_sql(gelocations_sql, pg_engine)

    products_sql = 'select * from olist.orskl_products'
    products = pd.read_sql(products_sql, pg_engine)

    sellers_sql = 'select * from olist.orskl_sellers'
    sellers = pd.read_sql(sellers_sql, pg_engine)

    translation_sql = 'select * from olist.orskl_translation'
    translation = pd.read_sql(translation_sql, pg_engine)

    order_max_timestamp_sql = 'select max(order_purchase_timestamp) from olist_staging.orskl_staging_rawdata'
    order_max_timestamp = pd.read_sql(order_max_timestamp_sql, pg_engine)
    order_max_time = order_max_timestamp['max'][0]
    orders_sql = "select * from olist.orskl_orders where order_purchase_timestamp > '"+str(order_max_time)+"'"
    orders = pd.read_sql(orders_sql, pg_engine)

    order_items_sql = "select * from olist.orskl_order_items where order_id in (select order_id from olist.orskl_orders where order_purchase_timestamp > '"+str(order_max_time)+"')"
    order_items = pd.read_sql(order_items_sql, pg_engine)

    order_payments_sql = "select * from olist.orskl_order_payments where order_id in (select order_id from olist.orskl_orders where order_purchase_timestamp > '"+str(order_max_time)+"')"
    order_payments = pd.read_sql(order_payments_sql, pg_engine)

    order_reviews_sql = "select * from olist.orskl_order_reviews where order_id in (select order_id from olist.orskl_orders where order_purchase_timestamp > '"+str(order_max_time)+"')"
    order_reviews = pd.read_sql(order_reviews_sql, pg_engine)

    geolocations_unique = geolocations[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']]. \
        groupby('geolocation_zip_code_prefix').agg({'geolocation_lat': 'min', 'geolocation_lng': 'min'}).reset_index()

    olist_final = orders.join(order_items.set_index('order_id'), on='order_id', how='left'). \
        join(products.set_index('product_id'), on='product_id', how='left'). \
        join(translation.set_index('product_category_name'), on='product_category_name', how='left'). \
        join(order_payments.set_index('order_id'), on='order_id', how='left'). \
        join(customers.set_index('customer_id'), on='customer_id', how='left'). \
        join(sellers.set_index('seller_id'), on='seller_id', how='left'). \
        merge(geolocations_unique, how='left', left_on='customer_zip_code_prefix',
              right_on='geolocation_zip_code_prefix',
              suffixes=("_x", "_y")). \
        merge(geolocations_unique, how='left', left_on='seller_zip_code_prefix',
              right_on='geolocation_zip_code_prefix'). \
        merge(order_reviews, how='left', on='order_id')

    olist_final.to_sql("orskl_staging_rawdata", con=conn, schema='olist_staging', index=False, if_exists='append')
    olist_attributes = {'earlier_max_timestamp': [order_max_time]}
    pd.DataFrame(olist_attributes).to_sql("pipelinetimestamp", con=conn, schema='olist_staging', index=False, if_exists='replace')





# Table 1
# Col1    Col2    Col3
# A         B       C
# B
#
# Table 2
# Col1    Col4    Col5
# A        E      F
# A        1      2
# A
# B
# B
# B
# B
#
# Table 1 left join table 2 on col1
#
# col1 col2 col3 col4 col5
# A  B  C E F
# A B C 1 2
































