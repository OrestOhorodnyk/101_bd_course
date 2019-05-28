name := "read_from_kafka"

version := "0.1"

scalaVersion := "2.11.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
//  "org.apache.spark" % "spark-streaming_2.11" % "2.3.1" % "provided"
)
