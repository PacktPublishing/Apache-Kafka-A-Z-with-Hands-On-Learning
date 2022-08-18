//package com.kafka.scala.demo8
//
//import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
//import backtype.storm.topology.base.BaseBasicBolt
//import backtype.storm.tuple.{Tuple, Values, Fields}
//
//
//class SplitLineBolt extends BaseBasicBolt{
//  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
//    val sentence = input.getString(0)
//    sentence.split(" ").foreach(
//      (word) => {
//        collector.emit(new Values(word))
//      }
//    )
//  }
//  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
//    declarer.declare(new Fields("word"))
//  }
//}
