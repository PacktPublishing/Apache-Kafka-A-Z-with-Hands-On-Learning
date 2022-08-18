//package com.kafka.scala.demo8
//
//import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
//import backtype.storm.topology.base.BaseBasicBolt
//import backtype.storm.tuple.{Tuple, Values, Fields}
//class CountWordBolt extends BaseBasicBolt {
//  val wordCountMap = scala.collection.mutable.Map[String, Int]()
//
//
//  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
//    val word = input.getString(0)
//    val wordCount = wordCountMap.get(word)
//    if (wordCount.isEmpty) {
//      wordCountMap.put(word, 1)
//    } else {
//      wordCountMap.put(word, wordCount.get + 1)
//    }
//    collector.emit(new Values(word, wordCountMap))
//    println("==========Word Map = "+wordCountMap)
//  }
//  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
//    declarer.declare(new Fields("word", "count"))
//  }
//}
