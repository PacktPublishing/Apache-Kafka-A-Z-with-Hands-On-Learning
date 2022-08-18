package com.kafka.scala.demo2.subdemo2

import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster

class CustomPartitioner() extends  DefaultPartitioner {

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {

//    (UUID.randomUUID().toString.hashCode + valueBytes.hashCode()) % cluster.partitionCountForTopic(topic)
    return 1
  }

}
