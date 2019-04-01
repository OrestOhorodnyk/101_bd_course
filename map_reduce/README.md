Environment:

hdphdp/3.0.1.0-187

python 3.6

Precondiditon:

Install python on CentOS:
1) `ssh root@localhost -p 2222`
2) `sudo yum install -y https://centos7.iuscommunity.org/ius-release.rpm`
3) `sudo yum update`
4) `sudo yum install -y python36u python36u-libs python36u-devel python36u-pip`

Setup:
1) Copy folder with source code to hdp 

 `scp -r -P 2222 <source>/mapReduce/ root@localhost:<destination>/mapReduce`
 
2) make all mappers and redusers executable (see example below)
 `
 chmod +x <destination>/mapReduce/ip/map_ip.py`
3) switch to hdfs user 

 `su hdfs`
4) copy log file to hdfs

 `hadoop fs -mkdir -p /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/input`
 `hadoop fs -put <destination>/mapReduce/access_logs/input/000000 /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/input`

RUN JOB:

1) no compretion and one reducer 

`hadoop jar /usr/hdp/3.0.1.0-187/hadoop-mapreduce/hadoop-streaming-3.1.1.3.0.1.0-187.jar \
-file ./<path to folder>/mapReduce/ip/map_ip.py  -mapper ./<path to folder>/mapReduce/ip/map_ip.py \
-file ./<path to folder>/mapReduce/ip/reduce_ip.py -reducer ./<path to folder>/mapReduce/ip/reduce_ip.py \
-input /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/input \
-output /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip`

2) no compretion and multiple reducer (e. g 16)

`hadoop jar /usr/hdp/3.0.1.0-187/hadoop-mapreduce/hadoop-streaming-3.1.1.3.0.1.0-187.jar \
-D mapred.reduce.tasks=16 \
-file ./<path to folder>/mapReduce/ip/map_ip.py  -mapper ./<path to folder>/mapReduce/ip/map_ip.py \
-file ./<path to folder>/mapReduce/ip/reduce_ip.py -reducer ./<path to folder>/mapReduce/ip/reduce_ip.py \
-input /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/input \
-output /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip`

3) compresed with Snappy 

` hadoop jar /usr/hdp/3.0.1.0-187/hadoop-mapreduce/hadoop-streaming-3.1.1.3.0.1.0-187.jar \
-D mapred.output.compress=true -D mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec \
-file ./<path to folder>/mapReduce/ip/map_ip.py  -mapper ./<path to folder>/mapReduce/ip/map_ip.py \
-file ./<path to folder>/mapReduce/ip/reduce_ip.py -reducer ./<path to folder>/mapReduce/ip/reduce_ip.py \
-input /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/input \
-output /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip`

To view results 

`hadoop fs -text hdfs://sandbox-hdp.hortonworks.com:8020/usr/hdp/3.0.1.0-187/usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip/part-00000.snappy`
To view limit number of rows (e. d. first 10 rows)

`hadoop fs -text hdfs://sandbox-hdp.hortonworks.com:8020/usr/hdp/3.0.1.0-187/usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip/part-00000.snappy | head 10`

to copy results from hdfs
 
`hadoop fs -copyToLocal /usr/hdp/3.0.1.0-187/hadoop-mapreduce/101_bd/output-reduce-ip <destination path>/101_bd_map_reduce/output`
