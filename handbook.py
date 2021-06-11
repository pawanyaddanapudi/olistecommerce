# Go get the data of 9 tables from postgresql into the python kernel as dataframes
# Join all the dataframes as needed
# Dump the final dataframe into a table in the olist_staging schema

import pandas as pd
from sqlalchemy import create_engine
import psycopg2

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
    join(sellers.set_index('seller_id'), on='seller_id', how='left').\
    merge(geolocations, how='left',
         left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', suffixes=('','_customer'))


geolocations_computed = geolocations.groupby('geolocation_zip_code_prefix').agg({'geolocation_lat':'min', 'geolocation_lng':'min'}).reset_index()
geolocations.columns

pd.to_datetime(orders['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S').\
    apply(lambda x: str(x.year)+str("{:02}".format(x.weekofyear)))


# Job 2

rawdata_sql = 'select * from olist_staging.orskl_olist_rawdata'
rawdata = pd.read_sql(rawdata_sql, pg_engine)



