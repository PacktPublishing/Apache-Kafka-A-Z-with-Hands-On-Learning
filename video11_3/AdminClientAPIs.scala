package com.kafka.scala.demo10

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl.{AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.resource.{PatternType, ResourceFilter, ResourcePatternFilter, ResourceType}
import org.codehaus.jackson.map.ObjectMapper;

object AdminClientAPIs {

  def main(args: Array[String]): Unit = {
    increaseTopicPartition()
  }

  def listTopics() : Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val kafkaAdmin = AdminClient.create(config)
    val topics : util.Iterator[TopicListing] = kafkaAdmin.listTopics().listings().get().iterator();
    while(topics.hasNext) {
      val topic:TopicListing = topics.next();
      print("\nTopic Name : "+topic.name())
    }
  }

  def createAdminClient() : AdminClient = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    AdminClient.create(config)
  }

  def createTopics() : Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val kafkaAdmin = AdminClient.create(config)
    val topicName = "test_topic"
    val replication:Short = 1
    val partition = 6

    val topicOptions:CreateTopicsOptions = new CreateTopicsOptions()
    topicOptions.timeoutMs(1000)

    val topicConfig : NewTopic =
      new NewTopic(topicName, partition, replication);

    val newTopics = Collections.singleton(topicConfig);

    kafkaAdmin.createTopics(newTopics, topicOptions).all().get();

    listTopics()
  }

  def deleteTopics() : Unit = {

    val kafkaAdmin = createAdminClient();
    val topicName = "monitor_topic"
    val topicOptions:DeleteTopicsOptions = new DeleteTopicsOptions()
    topicOptions.timeoutMs(1000)

    val topicToBeDeleted = Collections.singleton(topicName);

    kafkaAdmin.deleteTopics(topicToBeDeleted, topicOptions).all().get();

    listTopics();
  }


  def describeTopics() : Unit = {

    val kafkaAdmin = createAdminClient();
    val topicName = "topic10"
    val topicOptions:DescribeTopicsOptions = new DescribeTopicsOptions()
    topicOptions.timeoutMs(1000)

    val topicToBeDeleted = Collections.singleton(topicName);

    val topicsDetails:util.Iterator[util.Map.Entry[String, TopicDescription]] = kafkaAdmin.describeTopics(topicToBeDeleted, topicOptions).all().get().entrySet().iterator();

    while(topicsDetails.hasNext) {
      val topic = topicsDetails.next();
      print(new ObjectMapper().writeValueAsString(topic.getValue))
      topic.getValue.partitions().foreach{println}
    }
  }

  def describeACLs() : Unit = {
    val kafkaAdmin = createAdminClient();
    val any:AclBindingFilter =  new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "acl_topic", PatternType.ANY),
      new AccessControlEntryFilter("User:winsten", null, AclOperation.ANY, AclPermissionType.ANY));
    val result:DescribeAclsResult = kafkaAdmin.describeAcls(any);
    print("ACLs : "+result.values().get()+"\n")
  }

  def describeTopicConfig() : Unit = {
    val kafkaAdmin = createAdminClient();
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "monitor_events")
    val result:DescribeConfigsResult = kafkaAdmin.describeConfigs(Collections.singleton(configResource));
    print("Config : "+result.all().get()+"\n")
  }

  def describeBrokerConfig() : Unit = {
    val kafkaAdmin = createAdminClient();
    val configResource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    val result:DescribeConfigsResult = kafkaAdmin.describeConfigs(Collections.singleton(configResource));
    print("Config : "+result.all().get()+"\n")
  }


  def alertConfig() : Unit = {
    val kafkaAdmin = createAdminClient();
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "topic9")
    val retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "88000")
    val updateConfig = Collections.singletonMap(configResource, new Config(Collections.singleton(retentionEntry)));
    val result:AlterConfigsResult = kafkaAdmin.alterConfigs(updateConfig);
    result.all().get()
  }

  def describeCluster() : Unit = {
    val kafkaAdmin = createAdminClient();
    val result:DescribeClusterResult = kafkaAdmin.describeCluster();
    print("Nodes "+ result.nodes().get())
    print("\nCluster Id "+ result.clusterId().get())
    print("\nController  "+ result.controller().get())
  }

  def increaseTopicPartition() : Unit = {
    val kafkaAdmin = createAdminClient();
    val topic = "topic10"
    val result:CreatePartitionsResult = kafkaAdmin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(8)));
    describeTopics();
    print(result.values().get(topic).get())
    describeTopics();
  }


}
