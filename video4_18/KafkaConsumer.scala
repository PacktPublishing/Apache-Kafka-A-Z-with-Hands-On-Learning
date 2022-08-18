//package com.kafka.scala.demo3.subdemo2
//
//import java.time.Duration
//import java.util
//import java.util.Properties
//
//import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
//import org.apache.kafka.common.errors.WakeupException
//
//import scala.collection.JavaConversions._
//
//object KafkaConsumer {
//  def main(args: Array[String]): Unit = {
//    val topic = "demo_sub_topic_1";
//    consumeFromKafka(topic)
//  }
//
//  def consumeFromKafka(topic: String) = {
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "com.kafka.scala.demo3.subdemo2.StudentDeserializer")
//    props.put("auto.offset.reset", "latest")
//    props.put("group.id", "consumer-group")
//    var consumer : KafkaConsumer[String, Student] = new KafkaConsumer[String, Student](props);
//    consumer.subscribe(util.Arrays.asList(topic))
//    try {
//      while (true) {
//        val record:ConsumerRecords[String,Student] = consumer.poll(Duration.ofMillis(1000))
//        for (data <- record.iterator) {
//          println("Key = "+data.key())
//          println("Name = "+data.value().getName+" Age = "+data.value().getAge)
//        }
//      }
//    } catch {
//      case ex: WakeupException =>
//        ex.printStackTrace();
//    } finally {
//      consumer.close(); //commit offsets and inform group coordinator
//    }
//  }
//}
