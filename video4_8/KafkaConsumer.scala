package com.kafka.scala.demo3

import java.time.Duration
import java.util
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

object KafkaConsumer {

  var consumer : KafkaConsumer[String, String] = null;
  def main(args: Array[String]): Unit = {
    val topic = "monitor_events";
    consumeFromKafka(topic)
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")

    val consumerOffset = new util.HashMap[TopicPartition, OffsetAndMetadata]();
    consumer  = new KafkaConsumer[String, String](props);
    consumer.subscribe(util.Arrays.asList(topic))
    try {
      while (true) {
        val record:ConsumerRecords[String,String] = consumer.poll(Duration.ofMillis(1000))
        for (data <- record.iterator) {
          println("Key = "+data.key()+" Value = "+data.value())
        }
      }
    } catch {
      case ex: WakeupException =>
        ex.printStackTrace();
    } finally {
      consumer.close(); //commit offsets and inform group coordinator
    }
  }
  private def init(): Unit = {
    addShutdownHook(consumer);
  }

  def addShutdownHook(consumer: KafkaConsumer[String, String]): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        System.out.println("Starting exit...");
        consumer.wakeup();
        try {
          Thread.currentThread().join();
        }
        catch {
          case ex: InterruptedException =>
            ex.printStackTrace();
        }
      }
    });
  }


}
