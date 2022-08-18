package com.kafka.scala.demo2.subdemo1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val topic = "monitor_events"
    val producer = getKafkaProducer(topic)
    while(true) {
      val student:Student = new Student("Mike", 55)
      val record = new ProducerRecord[String, Student](topic, "Mike55", student)
      producer.send(record).get();
      Thread.sleep(1000)
    }
  }

  def getKafkaProducer(topic: String): KafkaProducer[String, Student] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "com.kafka.scala.demo2.subdemo1.StudentSerializer")
    new KafkaProducer[String, Student](props)
  }

}
