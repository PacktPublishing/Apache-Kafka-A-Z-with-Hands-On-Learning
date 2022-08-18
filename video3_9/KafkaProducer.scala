package com.kafka.scala.demo2.subdemo3

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val topic = "demo_topic_2";
    val producer = getKafkaProducer(topic);
    val record = new ProducerRecord[String, String](topic, "key11", "value4321")
    producer.send(record).get();
  }

  def getKafkaProducer(topic: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

}
