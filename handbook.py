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

data_path = '/Users/pawanyaddanapudi/GDrive_kumar_OrSkl/OrSklDataAnalytics/BigDataEngineering/BigDataEngineeringBooster/datasets/YouTubeTrendingVideos/'


CA_df = pd.read_csv(data_path+'CAvideos.csv', header=0)
CA_df['trending_date'] = pd.to_datetime(CA_df['trending_date'], format='%y.%d.%m')
CA_df_2017 = CA_df[CA_df['trending_date']<'2018-01-01']
CA_df_2018_1 = CA_df[(CA_df['trending_date']>='2018-01-01') & (CA_df['trending_date']<'2018-04-01')]
CA_df_2018_2 = CA_df[CA_df['trending_date']>='2018-04-01']
CA_df_2017.to_csv(data_path+'CAvideos_2017.csv',index=False, header=False)
CA_df_2018_1.to_csv(data_path+'CAvideos_2018_1.csv',index=False, header=False)
CA_df_2018_2.to_csv(data_path+'CAvideos_2018_2.csv',index=False, header=False)


DE_df = pd.read_csv(data_path+'DEvideos.csv', header=0)
DE_df['trending_date'] = pd.to_datetime(DE_df['trending_date'], format='%y.%d.%m')
DE_df_2017 = DE_df[DE_df['trending_date']<'2018-01-01']
DE_df_2018_1 = DE_df[(DE_df['trending_date']>='2018-01-01') & (DE_df['trending_date']<'2018-04-01')]
DE_df_2018_2 = DE_df[DE_df['trending_date']>='2018-04-01']
DE_df_2017.to_csv(data_path+'DEvideos_2017.csv',index=False, header=False)
DE_df_2018_1.to_csv(data_path+'DEvideos_2018_1.csv',index=False, header=False)
DE_df_2018_2.to_csv(data_path+'DEvideos_2018_2.csv',index=False, header=False)


FR_df = pd.read_csv(data_path+'DEvideos.csv', header=0)
FR_df['trending_date'] = pd.to_datetime(FR_df['trending_date'], format='%y.%d.%m')
FR_df_2017 = FR_df[FR_df['trending_date']<'2018-01-01']
FR_df_2018_1 = FR_df[(FR_df['trending_date']>='2018-01-01') & (FR_df['trending_date']<'2018-04-01')]
FR_df_2018_2 = FR_df[FR_df['trending_date']>='2018-04-01']
FR_df_2017.to_csv(data_path+'FRvideos_2017.csv',index=False, header=False)
FR_df_2018_1.to_csv(data_path+'FRvideos_2018_1.csv',index=False, header=False)
FR_df_2018_2.to_csv(data_path+'FRvideos_2018_2.csv',index=False, header=False)

from datetime import datetime
print(datetime.now().strftime('%y-%m-%d'))