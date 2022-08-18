package com.kafka.scala.demo2.subdemo1

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer

class StudentSerializer extends Serializer[Student] {

  var objectMapper:ObjectMapper = null;

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    objectMapper = new ObjectMapper();
  }

  override def serialize(topic: String, data: Student): Array[Byte] = {
    objectMapper.writeValueAsBytes(data)
  }

  override def close(): Unit = {
  }
}
