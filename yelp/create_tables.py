import constants
import json 
import requests 
import aws_keys
import pymysql
import pandas as pd
# import yelp_api
from yelp_api import connect_to_RDS_Yelp_DB
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

business_details_sql = '''
CREATE TABLE TEST_Business_Details (
    business_id varchar(50),
    business_alias varchar(100),
    business_name varchar(256),
    business_image_url varchar(500),
    business_is_claimed boolean,
    business_is_closed boolean,
    business_url varchar(500),
    business_phone varchar(15),
    business_display_phone varchar(15),
    business_review_count double,
    business_categories text,
    business_rating double,
    business_photos text,
    business_price varchar(5),
    business_hours MEDIUMTEXT,
    business_transactions text,
    business_address1 varchar(100),
    business_address2 varchar(50),
    business_address3 varchar(50),
    business_city varchar(50),
    business_zip_code varchar(10),
    business_country varchar(30),
    business_state varchar(20),
    business_display_address varchar(50),
    business_cross_streets text,
    business_latitude varchar(30),
    business_longitude varchar(30), 
    distance_from_search_in_meters double
)
'''


if __name__ == '__main__':
    cursor, conn = connect_to_RDS_Yelp_DB()


    # cursor.execute(business_details_sql)
    # output = cursor.fetchall()
    # print(output)

    # conn.close()

    cursor, conn = connect_to_RDS_Yelp_DB()
    sql = '''show tables'''
    cursor.execute(sql)
    output = cursor.fetchall()
    print(output)

    conn.close()
