package com.kafka.scala.demo2.subdemo4

import java.io.ByteArrayOutputStream
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val topic = "demo_topic_3";

    val schema: Schema = new Parser().parse("{\"namespace\":\"student.avro \",\"type\":\"record\",\"name\":\"Student\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}")
    // Create avro generic record object
    val genericUser: GenericRecord = new GenericData.Record(schema)
    //Put data in that generic record
    genericUser.put("name", "John Wick")
    genericUser.put("age", 1234)

    // Serialize generic record into byte array
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(genericUser, encoder)
    encoder.flush()
    out.close()

    val serializedBytes: Array[Byte] = out.toByteArray()
    val producer:KafkaProducer[String, Array[Byte]] = getKafkaProducer(topic)
    val record = new ProducerRecord[String, Array[Byte]](topic, "key11", serializedBytes)
    producer.send(record).get();
  }

  def getKafkaProducer(topic: String): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[String, Array[Byte]](props)
  }

}
