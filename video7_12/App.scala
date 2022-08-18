//package com.kafka.scala.demo6.subdemo1
//
//import java.util.Properties
//
//import com.sksamuel.avro4s.RecordFormat
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
//import org.apache.avro.generic.GenericRecord
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.scala.StreamsBuilder
//
//object App {
//
//  case class Employee(name: String, id: Long)
//
//  def main(args: Array[String]): Unit = {
//    val config = new Properties()
//    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "application_id")
//    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
//    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
//    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde].getCanonicalName)
//    config.put("schema.registry.url", "http://schemaregistry:8081")
//    val builder = new StreamsBuilder()
//    builder.stream[String, GenericRecord]("") .mapValues(v => RecordFormat[Employee].from(v))
//    (new KafkaStreams(builder.build(), config)).start();
//  }
//}
