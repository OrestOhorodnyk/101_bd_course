import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange, HasOffsetRanges}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe




object main {
  def main (args: Array[String] ): Unit = {

    val conf = new SparkConf().setAppName("SparkReadFromKafka").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(1))
//    val sparkContext = new SparkContext(conf)


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "id1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("201_streaming_hw")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))

    // Import dependencies and create kafka params as in Create Direct Stream above

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

  }

}
