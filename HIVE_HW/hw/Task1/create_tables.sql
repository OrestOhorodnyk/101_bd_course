
# create data base
CREATE SCHEMA IF NOT EXISTS 101_bd_hive;
USE 101_bd_hive;


#create external schema based on train.csv
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

#create table with the same fields as train_external
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

# copy data from external table
INSERT INTO TABLE 101_bd_hive.train SELECT * FROM 101_bd_hive.train_external;

DROP TABLE 101_bd_hive.train_external;