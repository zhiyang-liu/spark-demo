package com.example.stream

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    /**
      * 打包运行到集群
      * spark-submit --master spark://weekend110:7077 --name test1 --class com.example.stream.Demo3 ....jar
      * 需要将spark-streaming-kafka-0.8_2.1.1.jar和kafka_2.11-0.8.2.1.jar和zkclient.0.10.jar和metrics-core.2.1.1和kafka-clients-0.8.2.1.jar放入SPARKHOME/jars/   或者提交时指定 --jars ....
      */
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("/home/hadoop/checkpoint")//集群中默认在了hdfs
    val topics = List(("pandas", 1)).toMap
    val topicLines = KafkaUtils.createStream(ssc,
      "192.168.40.3:2181", //只写一个zk就可以，也可以写多个
      "testSparkStream", topics)
    val kafkaKeyValue = topicLines.map(e => e._2)
    kafkaKeyValue.print()
    val kafkaString = topicLines.map(e => e.toString())
    kafkaString.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
