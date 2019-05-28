This applications reads meseges from specific kafka topik and saves them to elasticsearch

# build
`mvn assembly:assembly -DdescriptorId=jar-with-dependencies`

# copy jar file to HDP sendbox
`scp -P 2222 kafka_to_elk-1.0-SNAPSHOT-jar-with-dependencies.jar  root@sandbox-hdp.hortonworks.com:/root`


# run on hdp sanbox
`spark-submit --class "consumer.App" --master local ./kafka_to_elk-1.0-SNAPSHOT-jar-with-dependencies.jar`