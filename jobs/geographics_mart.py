import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from haversine import haversine


def job6_table1():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    raw_data_sql = 'select * from olist_staging.orskl_staging_rawdata'
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    raw_data['distance'] = raw_data[['order_id','seller_id','customer_id','geolocation_lat_x',
                            'geolocation_lat_y', 'geolocation_lng_x','geolocation_lng_y']].\
    apply(lambda x: haversine((float(x['geolocation_lat_x']),float(x['geolocation_lng_x'])),
                              (float(x['geolocation_lat_y']),float(x['geolocation_lng_y'])))
          if (x['geolocation_lat_x'] and x['geolocation_lng_x'] and x['geolocation_lat_y'] and x['geolocation_lng_y']) else ''
          ,axis=1)

    raw_data[['order_id','seller_id','customer_id','distance']].to_sql('orskl_olist_geo_tab1', con=conn, schema='olist_geo_mart',index=False)


def job6_table1_inc():
    pg_engine = psycopg2.connect(dbname='airflow', host='postgres', port=5432, user='airflow', password='airflow')
    conn = create_engine('postgresql://airflow:airflow@postgres/airflow')
    get_max_time_sql = 'select * from olist_staging.pipelinetimestamp'
    max_date = pd.read_sql(get_max_time_sql, pg_engine)
    order_max_time = max_date['earlier_max_timestamp'][0]
    raw_data_sql = "select * from olist_staging.orskl_staging_rawdata where order_purchase_timestamp > '" + str(
        order_max_time) + "'"
    raw_data = pd.read_sql(raw_data_sql, pg_engine)
    raw_data['distance'] = raw_data[['order_id','seller_id','customer_id','geolocation_lat_x',
                            'geolocation_lat_y', 'geolocation_lng_x','geolocation_lng_y']].\
    apply(lambda x: haversine((float(x['geolocation_lat_x']),float(x['geolocation_lng_x'])),
                              (float(x['geolocation_lat_y']),float(x['geolocation_lng_y'])))
          if (x['geolocation_lat_x'] and x['geolocation_lng_x'] and x['geolocation_lat_y'] and x['geolocation_lng_y']) else ''
          ,axis=1)

    raw_data[['order_id','seller_id','customer_id','distance']].to_sql('orskl_olist_geo_tab1', con=conn, schema='olist_geo_mart',index=False, if_exists='append')

