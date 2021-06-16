import pandas as pd

data_path = '/Users/pawanyaddanapudi/GDrive_kumar_OrSkl/OrSklDataAnalytics/BigDataEngineering/BigDataEngineeringBooster/datasets/OlistECommerce/'


#pd.to_datetime(orskl_orders['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S').\
#    apply(lambda x: str(x.year)+str("{:02}".format(x.weekofyear)))

#pd.to_datetime(orskl_orders['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S').dt.week.apply(lambda x: "{:02}".format(x))

orskl_orders = pd.read_csv(data_path+'olist_orders_dataset.csv', header=0)
orskl_orders['order_purchase_timestamp'] = pd.to_datetime(orskl_orders['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S')

orskl_orders_20162017 = orskl_orders[orskl_orders['order_purchase_timestamp'] < '2018-01-01']
orskl_orders_2018_1 = orskl_orders[(orskl_orders['order_purchase_timestamp'] >= '2018-01-01') & (orskl_orders['order_purchase_timestamp'] < '2018-07-01')]
orskl_orders_2018_2 = orskl_orders[(orskl_orders['order_purchase_timestamp'] >= '2018-07-01')]



orskl_orders_20162017.to_csv(data_path+'olist_orders_dataset_20162017.csv', index=False)
orskl_orders_2018_1.to_csv(data_path+'olist_orders_dataset_2018_1.csv', index=False)
orskl_orders_2018_2.to_csv(data_path+'olist_orders_dataset_2018_2.csv', index=False)



orskl_orders_items = pd.read_csv(data_path+'olist_order_items_dataset.csv', header=0)

orskl_orders_items_20162017 = orskl_orders_items[orskl_orders_items['order_id'].isin(orskl_orders_20162017['order_id'])]
orskl_orders_items_2018_1 = orskl_orders_items[orskl_orders_items['order_id'].isin(orskl_orders_2018_1['order_id'])]
orskl_orders_items_2018_2 = orskl_orders_items[orskl_orders_items['order_id'].isin(orskl_orders_2018_2['order_id'])]


orskl_orders_items_20162017.to_csv(data_path+'olist_orders_items_dataset_20162017.csv', index=False)
orskl_orders_items_2018_1.to_csv(data_path+'olist_orders_items_dataset_2018_1.csv', index=False)
orskl_orders_items_2018_2.to_csv(data_path+'olist_orders_items_dataset_2018_2.csv', index=False)


orskl_reviews =  pd.read_csv(data_path+'olist_order_reviews_dataset.csv', header=0)
orskl_reviews['review_creation_date'] = pd.to_datetime(orskl_reviews['review_creation_date'], format='%Y-%m-%d %H:%M:%S')
orskl_reviews_20162017 = orskl_reviews[orskl_reviews['review_creation_date'] < '2018-01-01']
orskl_reviews_2018_1 = orskl_reviews[(orskl_reviews['review_creation_date'] >= '2018-01-01') & (orskl_reviews['review_creation_date'] < '2018-07-01')]
orskl_reviews_2018_2 = orskl_reviews[(orskl_reviews['review_creation_date'] >= '2018-07-01')]

orskl_reviews_20162017[['review_id','order_id','review_score','review_creation_date','review_answer_timestamp']].to_csv(data_path+'olist_order_reviews_dataset_20162017.csv', index=False, encoding="utf-8-sig")
orskl_reviews_2018_1[['review_id','order_id','review_score','review_creation_date','review_answer_timestamp']].to_csv(data_path+'olist_order_reviews_dataset_2018_1.csv', index=False)
orskl_reviews_2018_2[['review_id','order_id','review_score','review_creation_date','review_answer_timestamp']].to_csv(data_path+'olist_order_reviews_dataset_2018_2.csv', index=False)


orskl_payments = pd.read_csv(data_path+'olist_order_payments_dataset.csv', header=0)
orskl_payments_20162017 = orskl_payments[orskl_payments['order_id'].isin(orskl_orders_20162017['order_id'])]
orskl_payments_2018_1 = orskl_payments[orskl_payments['order_id'].isin(orskl_orders_2018_1['order_id'])]
orskl_payments_2018_2 = orskl_payments[orskl_payments['order_id'].isin(orskl_orders_2018_2['order_id'])]

orskl_payments_20162017.to_csv(data_path+'olist_order_payments_dataset_20162017.csv', index=False)
orskl_payments_2018_1.to_csv(data_path+'olist_order_payments_dataset_2018_1.csv', index=False)
orskl_payments_2018_2.to_csv(data_path+'olist_order_payments_dataset_2018_2.csv', index=False)

