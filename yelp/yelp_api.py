import constants
import json 
import requests 
import aws_keys
import pymysql
import pandas as pd
from sqlalchemy import create_engine


# api key from constants
YELP_API_KEY = constants.YELP_API_KEY
YELP_API_CLIENT_ID = constants.YELP_API_CLIENT_ID

# Global constants
YELP_BASE_PATH = 'https://api.yelp.com'
YELP_SEARCH_PATH = '/v3/businesses/search'
YELP_BUSINESS_PATH = '/v3/businesses/' 
YELP_DELIVERY_PATH = '/v3/transactions/delivery/search'

# Defaults for our simple example.
YELP_DEFAULT_TERM = 'dinner'
YELP_DEFAULT_LOCATION = 'New York, NY'
# YELP_DEFAULT_BUSINESS_ID = 'VJ4fKEeTljlRgCTfYdcs_Q'  # North Italia in Charlotte
YELP_SEARCH_LIMIT = 20

RDS_HOST = aws_keys.YELP_RDS_HOST
RDS_USER = aws_keys.YELP_RDS_MASTER_USERNAME
RDS_PASSWORD = aws_keys.YELP_RDS_MASTER_PASSWORD
RDS_DATABASE = aws_keys.YELP_RDS_NAME


def connect_to_RDS_Yelp_DB():
    connection = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, db=RDS_DATABASE) # , RDS_DATABASE)   
    return connection.cursor(), connection

def get_request(api_key, rel_path, base_path, params=None):
    ''' Uses python requests to get information back from the Yelp Fusion API
    
    Arguments:
        - api_key: YELP_API_KEY stored in a hidden constants.py file
        - rel_path: relative api path for the specific api call
        - base_path: base yelp api path
        - params: the url params to pass to the get request
    '''
    url = f'{base_path}{rel_path}'
    url_params = params or {}
    auth = {
        'Authorization' : f'Bearer {api_key}'
    }
    print(f"url: {url}")

    response = requests.get(url=url, headers=auth,params=url_params)
    return response.json()


def search_businesses(api_key, term, location):
    '''
    Arguments:

    '''
    params = {
        'location' : location,
        'term' : term,
        'limit' : YELP_SEARCH_LIMIT
    }
    return get_request(api_key, YELP_SEARCH_PATH, YELP_BASE_PATH, params)


def search_delivery(api_key, term, location):
    '''
    Arguments:

    '''
    params = {
        'location' : location,
        'term' : term,
        'limit' : YELP_SEARCH_LIMIT,
    }
    return get_request(api_key, YELP_DELIVERY_PATH, YELP_BASE_PATH, params)


def get_business_details(api_key, business_id):
    ''' Get the details of a specific business

    Arguments:
    '''
    business_id_path = YELP_BUSINESS_PATH + business_id
    return get_request(api_key, business_id_path, YELP_BASE_PATH)


def get_business_review(api_key, business_id):

    business_review_path = YELP_BUSINESS_PATH + business_id + '/reviews'
    return get_request(api_key, business_review_path, YELP_BASE_PATH)


def write_business_details_to_RDS(cursor, conn, api_key, business_id):
    json_response = get_business_details(api_key, business_id)

    normalized_biz_dtl = pd.json_normalize(json_response)
    print(normalized_biz_dtl.columns)

    # full_col_list = ['id', 'alias', 'name', 'image_url', 'is_closed', 'url', 'review_count', 'categories', 'rating', 'categories.alias', 'categories.title',
    #    'rating', 'coordinates.latitude', 'coordinates.longitude', 'transactions', 'price', 'location.address1', 'location.address2', 'location.address3', 'location.city',
    #    'location.zip_code', 'location.country', 'location.state', 'location.display_address', 'location.cross_streets',   
    #    'phone', 'display_phone', 'distance', 'hours', 'attributes', 'is_claimed', 'date_opened', 'date_closed', 'photos', 'special_hours.date', 'special_hours.start', 'special_hours.end',
    #    'special_hours.is_overnight', 'special_hours.is_closed', 'messaging.url', 'messaging.use_case_text', 'messaging.response_rate', 'messaging.response_time', 'messaging.is_enabled',
    #    'photo_count', 'photo_details.photo_id', 'photo_details.url', 'photo_details.caption', 'photo_details.width', 'photo_details.height', 'photo_details.is_user_submitted', 
    #    'photo_details.user_id', 'photo_details.label', 'yelp_menu_url', 'cbsa', 'popularity_score.primary_category', 'popularity_score.score',
    #    'rapc.is_enabled', 'rapc.is_eligible'
    #    ]
    full_col_list = ['id', 'alias', 'name', 'image_url', 'is_claimed', 'is_closed', 'url', 'phone', 'display_phone', 'review_count', 'categories', 'rating','photos', 'price', 'hours', 'transactions', 'location.address1',
       'location.address2', 'location.address3', 'location.city','location.zip_code', 'location.country', 'location.state','location.display_address', 'location.cross_streets','coordinates.latitude', 'coordinates.longitude']

    # add blank column if not in
    for c in full_col_list:
        if c not in normalized_biz_dtl:
            normalized_biz_dtl[c] = None
    
    normalized_biz_dtl = normalized_biz_dtl.rename(columns={
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
    
    print(normalized_biz_dtl)

    normalized_biz_dtl.to_sql('TEST_Business_Details', con=conn, if_exists='append')
    

if __name__ == '__main__':
    # print(search_businesses(YELP_API_KEY, 'italian', 'Charlotte, NC')) # get three restaurants
    # business_detail = get_business_details(YELP_API_KEY, 'jcasci3gjbsSuTEVzvDQKg')
    # print(get_business_details(YELP_API_KEY, 'jcasci3gjbsSuTEVzvDQKg')) # Mama Ricotta's
    # print(get_business_review(YELP_API_KEY, 'jcasci3gjbsSuTEVzvDQKg')) # Mama Ricotta's review
    # print(search_delivery(YELP_API_KEY,'mexican','West Village, New York'))
    # print(get_request(YELP_API_KEY, YELP_SEARCH_PATH, YELP_BASE_PATH))



    cursor, conn = connect_to_RDS_Yelp_DB()

    write_business_details_to_RDS(cursor, conn,YELP_API_KEY, 'jcasci3gjbsSuTEVzvDQKg')

    # sql = '''show tables'''
    # cursor.execute(sql)
    # output = cursor.fetchall()
    # print(output)

    conn.close()