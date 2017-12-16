name := "MRSpark2"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" ,
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0",
  "org.apache.spark" % "spark-yarn_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.5.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0"
)