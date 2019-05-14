CREATE SCHEMA IF NOT EXISTS 101_bd_hive;
USE 101_bd_hive;

CREATE EXTERNAL TABLE IF NOT EXISTS 101_bd_hive.text_external (
id STRING,
date_time TIMESTAMP,
site_name STRING,
posa_continent STRING,
user_location_country STRING,
user_location_region STRING,
user_location_city STRING,
orig_destination_distance FLOAT,
user_id STRING, 
is_mobile STRING,
is_package BOOLEAN,
channel STRING, 
srch_ci DATE,
srch_co DATE, 
srch_adults_cnt INT,
srch_children_cnt INT,
srch_rm_cnt INT,
srch_destination_id STRING,
srch_destination_type_id STRING,
hotel_continent STRING,
hotel_country STRING,
hotel_market STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/usr/admin/test_data/etc'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS 101_bd_hive.train_external (
date_time TIMESTAMP,
site_name STRING,
posa_continent STRING,
user_location_country STRING,
user_location_region STRING,
user_location_city STRING,
orig_destination_distance FLOAT,
user_id STRING,
is_mobile BOOLEAN,
is_package BOOLEAN,
channel STRING,
srch_ci DATE,
srch_co DATE,
srch_adults_cnt INT,
srch_children_cnt INT,
srch_rm_cnt INT, 
srch_destination_id STRING, 
srch_destination_type_id STRING,
is_booking INT,
cnt INT,
hotel_continent STRING,
hotel_country STRING,
hotel_market STRING,
hotel_cluster STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/data/train'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS 101_bd_hive.train (
date_time TIMESTAMP,
site_name STRING,
posa_continent STRING,
user_location_country STRING,
user_location_region STRING,
user_location_city STRING,
orig_destination_distance FLOAT,
user_id STRING,
is_mobile BOOLEAN,
is_package BOOLEAN,
channel STRING,
srch_ci DATE,
srch_co DATE,
srch_adults_cnt INT,
srch_children_cnt INT,
srch_rm_cnt INT, 
srch_destination_id STRING, 
srch_destination_type_id STRING,
is_booking INT,
cnt INT,
hotel_continent STRING,
hotel_country STRING,
hotel_market STRING,
hotel_cluster STRING
)
STORED AS ORC;

INSERT INTO TABLE 101_bd_hive.train SELECT * FROM 101_bd_hive.train_external;


SELECT 


37 670 293
INSERT INTO TABLE 101_bd_hive.train SELECT * FROM 101_bd_hive.train_external where rand() <= 0.0001 distribute by rand() sort by rand() limit 10000;




