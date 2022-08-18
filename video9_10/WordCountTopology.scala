//package com.kafka.scala.demo8
//
//import backtype.storm.{Config, LocalCluster}
//import backtype.storm.spout.SchemeAsMultiScheme
//import backtype.storm.topology.TopologyBuilder
//import backtype.storm.tuple.Fields
//import storm.kafka.{KafkaSpout, SpoutConfig, StringScheme, ZkHosts}
//
//object WordCountTopology {
//
//  def main(args: Array[String]): Unit = {
//    val hosts = new ZkHosts("localhost:2181")
//    val kafkaConfig = new SpoutConfig(hosts, "sourceTopic", "/kafka", "storm");
//    kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme())
//    val kafkaSpout = new KafkaSpout(kafkaConfig)
//
//    val builder = new TopologyBuilder
//    builder.setSpout("events-from-kafka", kafkaSpout, 4)
//    builder.setBolt("split",
//      new SplitLineBolt, 4).shuffleGrouping("events-from-kafka")
//    builder.setBolt("count", new CountWordBolt, 1).fieldsGrouping("split",
//      new Fields("word"))
//    val conf = new Config
//    conf.setDebug(true)
//    conf.setMaxTaskParallelism(3)
//
//    val localCluster = new LocalCluster()
//    localCluster.submitTopology("word-count", conf, builder.createTopology())
//    Runtime.getRuntime.addShutdownHook(new Thread() {
//      override def run(): Unit = {
//        localCluster.killTopology("word-count")
//        localCluster.shutdown
//      }
//    })
//  }
//
//}
