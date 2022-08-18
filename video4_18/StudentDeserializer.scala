package com.kafka.scala.demo3.subdemo2

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class StudentDeserializer extends Deserializer[Student] {

  var objectMapper:ObjectMapper = null

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    objectMapper = new ObjectMapper()
  }

  override def deserialize(topic: String, data: Array[Byte]): Student = {
    return objectMapper.readValue(data, classOf[Student])
  }

  override def close(): Unit = {
  }
}
