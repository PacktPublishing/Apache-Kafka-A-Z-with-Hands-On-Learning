//package com.kafka.scala.demo7
//
//import java.time.Duration
//import java.util.Properties
//
//import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.scala.ImplicitConversions._
//import org.apache.kafka.streams.scala.Serdes._
//import org.apache.kafka.streams.scala.StreamsBuilder
//import org.apache.kafka.streams.scala.kstream.KTable
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//
//object KTables {
//
//  def main(args: Array[String]): Unit = {
//
//    val props = new Properties
//    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "sample-stream-application-11")
//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
//
//    val builder = new StreamsBuilder
//    val stream: KTable[String,Long] =
//      builder.stream[String, String]("destinationTopic")
//        .flatMapValues((value) => value.split(" ").toList)
//        .groupBy((key,word) => word)
//        .count()
//    stream.toStream.foreach((key, value) => {
//      println("" +key +" Count = "+value)
//    })
//    val streams = new KafkaStreams(builder.build(), props)
//    streams.start
//    sys.ShutdownHookThread {
//      streams.close(Duration.ofSeconds(5))
//    }
//  }
//}
//
