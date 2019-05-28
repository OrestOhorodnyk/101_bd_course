package consumers

class ComsumerConfig {
  private val  kafkaPort: String = "6667"
  private val kafkaHost: String = "sandbox-hdp.hortonworks.com"
  private val topicNames: List[String] = List("201_streaming_hw")


  def getKafkaHost: String ={
    "%1s:%2s".format(kafkaHost, kafkaPort)
  }

//  def getTopics: List[String]={
//    topicNames
//  }

//  def addTopic(topic_name:String):{
//    topicNames
//  }

}
