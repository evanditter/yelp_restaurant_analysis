import json
import pymysql
import pandas as pd
import aws_keys
from yelp_api import connect_to_RDS_Yelp_DB
from sqlalchemy import create_engine


test_json_str = {'id': 'jcasci3gjbsSuTEVzvDQKg', 'alias': 'mama-ricottas-charlotte-16', 'name': "Mama Ricotta's", 'image_url': 'https://s3-media2.fl.yelpcdn.com/bphoto/qmQwtiFSe_RoB1Gz7XlVdA/o.jpg', 'is_claimed': True, 'is_closed': False, 'url': 'https://www.yelp.com/biz/mama-ricottas-charlotte-16?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_lookup&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 'phone': '+17043430148', 'display_phone': '(704) 343-0148', 'review_count': 1125, 'categories': [{'alias': 'italian', 'title': 'Italian'}, {'alias': 'venues', 'title': 'Venues & Event Spaces'}], 'rating': 4.2, 'location': {'address1': '601 S Kings Dr Aa', 'address2': 'Ste AA', 'address3': '', 'city': 'Charlotte', 'zip_code': '28204', 'country': 'US', 'state': 'NC', 'display_address': ['601 S Kings Dr Aa', 'Ste AA', 'Charlotte, NC 28204'], 'cross_streets': ''}, 'coordinates': {'latitude': 35.2098855155106, 'longitude': -80.8355979085983}, 'photos': ['https://s3-media2.fl.yelpcdn.com/bphoto/qmQwtiFSe_RoB1Gz7XlVdA/o.jpg', 'https://s3-media1.fl.yelpcdn.com/bphoto/A3-vEriQJ_CiR-YbCwcZVA/o.jpg', 'https://s3-media1.fl.yelpcdn.com/bphoto/hNQsbhIt1iG6WVtAuGt8iQ/o.jpg'], 'price': '$$', 'hours': [{'open': [{'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 0}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 1}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 2}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 3}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 4}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 5}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 6}], 'hours_type': 'REGULAR', 'is_open_now': True}], 'transactions': ['pickup', 'delivery']}

norm = pd.json_normalize(test_json_str)

# print(norm.columns)

full_col_list = ['id', 'alias', 'name', 'image_url', 'is_closed', 'url', 'review_count', 'categories', 'rating', 'categories.alias', 'categories.title',
       'rating', 'coordinates.latitude', 'coordinates.longitude', 'transactions', 'price', 'location.address1', 'location.address2', 'location.address3', 'location.city',
       'location.zip_code', 'location.country', 'location.state', 'location.display_address', 'location.cross_streets',   
       'phone', 'display_phone', 'distance', 'hours', 'attributes', 'is_claimed', 'date_opened', 'date_closed', 'photos', 'special_hours.date', 'special_hours.start', 'special_hours.end',
       'special_hours.is_overnight', 'special_hours.is_closed', 'messaging.url', 'messaging.use_case_text', 'messaging.response_rate', 'messaging.response_time', 'messaging.is_enabled',
       'photo_count', 'photo_details.photo_id', 'photo_details.url', 'photo_details.caption', 'photo_details.width', 'photo_details.height', 'photo_details.is_user_submitted', 
       'photo_details.user_id', 'photo_details.label', 'yelp_menu_url', 'cbsa', 'popularity_score.primary_category', 'popularity_score.score',
       'rapc.is_enabled', 'rapc.is_eligible'
       ]

# orig column list
#['id', 'alias', 'name', 'image_url', 'is_claimed', 'is_closed', 'url',
    #    'phone', 'display_phone', 'review_count', 'categories', 'rating',
    #    'photos', 'price', 'hours', 'transactions', 'location.address1',
    #    'location.address2', 'location.address3', 'location.city',
    #    'location.zip_code', 'location.country', 'location.state',
    #    'location.display_address', 'location.cross_streets',
    #    'coordinates.latitude', 'coordinates.longitude']

for c in full_col_list:
    if c not in norm:
        norm[c] = None

# print(norm.columns)

norm = norm.rename(columns={
    'id' : 'business_id', 
    'alias' : 'business_alias', 
    'name' : 'business_name', 
    'image_url' : 'business_image_url', 
    'is_closed' : 'business_is_closed', 
    'url' : 'business_url',
    'phone' : 'business_phone', 
    'display_phone' : 'business_display_phone', 
    'review_count' : 'business_review_count', 
    'categories' : 'business_categories',
    'rating' : 'business_rating', 
    'photos' : 'business_photos',  
    'price' : 'business_price',  
    'hours' : 'business_hours',  
    'transactions' : 'business_transactions',  
    'location.address1' : 'business_address1', 
    'location.address2' : 'business_address2',  
    'location.address3' : 'business_address3',  
    'location.city' : 'business_city', 
    'location.zip_code' : 'business_zip_code',  
    'location.country' : 'business_country',  
    'location.state' : 'business_state', 
    'location.display_address' : 'business_display_address',  
    'location.cross_streets' : 'business_cross_streets', 
    'coordinates.latitude' : 'business_latitude',  
    'coordinates.longitude' : 'business_longitude',
    'is_claimed' : 'business_is_claimed', 
    'distance' : 'distance_from_search_in_meters'
})

print(norm.columns)
# print(norm['business_categories'].tolist())


# YELP_RDS_NAME = 'yelp_db'
# YELP_RDS_MASTER_USERNAME = 'admin' 
# YELP_RDS_MASTER_PASSWORD = 'needyelphelp'
# YELP_RDS_HOST= 'yelp-db.crcwsy4qwkgb.us-east-1.rds.amazonaws.com'


RDS_HOST = aws_keys.YELP_RDS_HOST
RDS_USER = aws_keys.YELP_RDS_MASTER_USERNAME
RDS_PASSWORD = aws_keys.YELP_RDS_MASTER_PASSWORD
RDS_DATABASE = aws_keys.YELP_RDS_NAME


def connect_to_RDS_Yelp_DB():
    connection = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, db=RDS_DATABASE) # , RDS_DATABASE)   
    return connection.cursor(), connection

rds_cursor, rds_conn = connect_to_RDS_Yelp_DB()

sql = '''show tables'''
rds_cursor.execute(sql)
output = rds_cursor.fetchall()
print(output)

connection_string = f"""mysql+pymysql://admin:needyelphelp@yelp-db.crcwsy4qwkgb.us-east-1.rds.amazonaws.com:3306/yelp_db"""
engine = create_engine(connection_string, echo=True)

# engine = create_engine(f"mysql+pymysql://{aws_keys.YELP_RDS_MASTER_USERNAME}:{aws_keys.YELP_RDS_MASTER_PASSWORD}@{aws_keys.YELP_RDS_HOST}:3306/{aws_keys.YELP_RDS_NAME}")

with engine.connect() as connection:
    test = connection.execute("SHOW TABLES")
    print(test)
    print(test.fetchall())

# df = pd.read_sql("SHOW TABLES", con = engine)
# print(df)
norm.to_sql('TEST_Business_Details', con=engine, if_exists='append', index=False)

# print(norm)





# {'id': 'jcasci3gjbsSuTEVzvDQKg', 
#  'alias': 'mama-ricottas-charlotte-16', 
#  'name': "Mama Ricotta's", 
#  'image_url': 'https://s3-media2.fl.yelpcdn.com/bphoto/qmQwtiFSe_RoB1Gz7XlVdA/o.jpg', 
#  'is_claimed': True, 
#  'is_closed': False, 
#  'url': 'https://www.yelp.com/biz/mama-ricottas-charlotte-16?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_lookup&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 
#  'phone': '+17043430148', 'display_phone': '(704) 343-0148', 'review_count': 1125, 'categories': [{'alias': 'italian', 'title': 'Italian'}, {'alias': 'venues', 'title': 'Venues & Event Spaces'}], 
#  'rating': 4.2, 
#  'location': {'address1': '601 S Kings Dr Aa', 'address2': 'Ste AA', 'address3': '', 'city': 'Charlotte', 'zip_code': '28204', 'country': 'US', 'state': 'NC', 'display_address': ['601 S Kings Dr Aa', 'Ste AA', 'Charlotte, NC 28204'], 'cross_streets': ''}, 
#  'coordinates': {'latitude': 35.2098855155106, 'longitude': -80.8355979085983}, 
#  'photos': ['https://s3-media2.fl.yelpcdn.com/bphoto/qmQwtiFSe_RoB1Gz7XlVdA/o.jpg', 'https://s3-media1.fl.yelpcdn.com/bphoto/A3-vEriQJ_CiR-YbCwcZVA/o.jpg', 'https://s3-media1.fl.yelpcdn.com/bphoto/hNQsbhIt1iG6WVtAuGt8iQ/o.jpg'], 
#  'price': '$$', 
#  'hours': [{'open': 
#             [{'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 0}, 
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 1}, 
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 2}, 
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 3}, 
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 4},  
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 5}, 
#              {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 6}], 
#         'hours_type': 'REGULAR', 'is_open_now': True}], 
#  'transactions': ['delivery', 'pickup']}
#                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 5}, {'is_overnight': False, 'start': '1100', 'end': '2100', 'day': 6}], 'hours_type': 'REGULAR', 'is_open_now': True}], 'transactions': ['delivery', 'pickup']}


# {'id': 'gW1BF2b4YhvL5bEezasNgQ', 
#  'alias': 'mano-bella-charlotte', 'name': 'Mano Bella', 
#  'image_url': 'https://s3-media1.fl.yelpcdn.com/bphoto/DOYmGrYMO4HJSx_CjBsr4w/o.jpg', 
#  'is_closed': False, 
#  'url': 'https://www.yelp.com/biz/mano-bella-charlotte?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 
#  'review_count': 1, 
#  'categories': [{'alias': 'italian', 'title': 'Italian'}, {'alias': 'salad', 'title': 'Salad'}, {'alias': 'sandwiches', 'title': 'Sandwiches'}], 
#  'rating': 4.0, 
#  'coordinates': {'latitude': 35.157133, 'longitude': -80.824604}, 
#  'transactions': ['pickup', 'delivery'], 
#  'location': {'address1': '721 Gov Morrison St', 'address2': '', 'address3': None, 'city': 'Charlotte', 'zip_code': '28211', 'country': 'US', 'state': 'NC', 
#               'display_address': ['721 Gov Morrison St', 'Charlotte, NC 28211']}, 
#  'phone': '+17049006506', 
#  'display_phone': '(704) 900-6506', 
#  'distance': 4410.378492774861}, 
# {'id': 'Z072pEHdos2Gjj3nMdR6uA', 'alias': 'luce-restaurant-and-bar-charlotte', 'name': 'Luce Restaurant & Bar', 'image_url': 'https://s3-media2.fl.yelpcdn.com/bphoto/OGxT2mR0D0AXoh3ZRNbc7g/o.jpg', 'is_closed': False, 'url': 'https://www.yelp.com/biz/luce-restaurant-and-bar-charlotte?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 'review_count': 276, 'categories': [{'alias': 'italian', 'title': 'Italian'}], 'rating': 4.2, 'coordinates': {'latitude': 35.22777, 'longitude': -80.84071}, 'transactions': ['delivery'], 'price': '$$$', 'location': {'address1': '214 N Tryon St', 'address2': 'Ste J', 'address3': '', 'city': 'Charlotte', 'zip_code': '28202', 'country': 'US', 'state': 'NC', 'display_address': ['214 N Tryon St', 'Ste J', 'Charlotte, NC 28202']}, 'phone': '+17043449222', 'display_phone': '(704) 344-9222', 'distance': 3641.253422740771}, 
# {'id': 'QReGruZEFxovIm9FJVa1kw', 'alias': 'zio-casual-italian-charlotte', 'name': 'Zio Casual Italian', 'image_url': 'https://s3-media2.fl.yelpcdn.com/bphoto/ucxUUiqQCMXADnlKWvolyA/o.jpg', 'is_closed': False, 'url': 'https://www.yelp.com/biz/zio-casual-italian-charlotte?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 'review_count': 189, 'categories': [{'alias': 'italian', 'title': 'Italian'}, {'alias': 'pizza', 'title': 'Pizza'}], 'rating': 4.0, 'coordinates': {'latitude': 35.197518, 'longitude': -80.825545}, 'transactions': ['delivery'], 'price': '$$', 'location': {'address1': '116 Middleton Dr', 'address2': '', 'address3': '', 'city': 'Charlotte', 'zip_code': '28207', 'country': 'US', 'state': 'NC', 'display_address': ['116 Middleton Dr', 'Charlotte, NC 28207']}, 'phone': '+17043440100', 'display_phone': '(704) 344-0100', 'distance': 480.8057891995271}, 
# {'id': 'eiUE9RMAdWSMptNnE-SfMw', 'alias': 'the-jimmy-charlotte', 'name': 'The Jimmy', 'image_url': 'https://s3-media1.fl.yelpcdn.com/bphoto/NRH9lGmmutcjiJtf_fiB2g/o.jpg', 'is_closed': False, 'url': 'https://www.yelp.com/biz/the-jimmy-charlotte?adjust_creative=oXQ7a8-EvOQOKqugDZIQYw&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=oXQ7a8-EvOQOKqugDZIQYw', 'review_count': 75, 'categories': [{'alias': 'mediterranean', 'title': 'Mediterranean'}, {'alias': 'italian', 'title': 'Italian'}, {'alias': 'pizza', 'title': 'Pizza'}], 'rating': 4.0, 'coordinates': {'latitude': 35.17408720008127, 'longitude': -80.83986580674186}, 'transactions': ['delivery'], 'location': {'address1': '2839 Selwyn Ave', 'address2': '', 'address3': None, 'city': 'Charlotte', 'zip_code': '28209', 'country': 'US', 'state': 'NC', 'display_address': ['2839 Selwyn Ave', 'Charlotte, NC 28209']}, 'phone': '+17049794242', 'display_phone': '(704) 979-4242', 'distance': 2588.471928573856}], 'total': 804, 'region': {'center': {'longitude': -80.83053588867188, 'latitude': 35.19608159407329}}}