This applications reads meseges from specific kafka topik and saves them to HDFS as parquet file

# build
`mvn assembly:assembly -DdescriptorId=jar-with-dependencies`


# run on hdp sanbox
`spark-submit --class "test.app1.App" --master local ./app1-0.0.3-SNAPSHOT-jar-with-dependencies.jar `
