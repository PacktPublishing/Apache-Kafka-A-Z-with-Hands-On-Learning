package com.kafka.scala.demo9.subdemo1

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext}

object SparkStreamKafkaDemo {

  def main(args: Array[String]) {
    val brokerUrl = "localhost:9092"
    val topicName = "sourceTopic"
    val groupId = "group-1"
    val sparkMaster = "local"
    val duration = 60
    val N = 5
    val sparkConf = new SparkConf
    sparkConf.setAppName("Spark Streaming App - TOP " + N + " words in last " + duration + " seconds")
    sparkConf.setMaster(if (sparkMaster.indexOf("local") != -1) "local[*]"
    else sparkMaster)

    val streamingContext = new StreamingContext(sparkConf, Seconds(duration))
    val kafkaParams = collection.mutable.Map[String, Object]()
    kafkaParams.put("bootstrap.servers", brokerUrl)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", groupId)
    kafkaParams.put("auto.offset.reset", "latest")
    kafkaParams.put("enable.auto.commit", "false")
    val topics = Array(topicName)

    val kafkaStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParams))
    val allLines = kafkaStream.map((record) => (record.value))

    val wordCount = allLines.flatMap(_.split(" ")).map((record) => (record.toString, 1)).reduceByKey((i1, i2) => i1 + i2)
    val topNWords = wordCount.transform((rdd) => {
      rdd.sortBy(_._2, false)
    })
    topNWords.print(N)
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

