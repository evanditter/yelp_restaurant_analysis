-- BUSINESS DETAIL
CREATE TABLE Business_Details (
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
    distance_from_search_in_meters double,
)