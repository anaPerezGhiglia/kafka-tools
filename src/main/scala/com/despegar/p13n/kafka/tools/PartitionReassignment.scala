package com.despegar.p13n.kafka.tools

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class TopicPartitionReplicas(topic: String, partition: Int, replicas: List[Int])

trait ToJson {
  def toJson = PartitionReassignment.mapper.writeValueAsString(this)

}
case class PartitionReassignment(partitions: List[TopicPartitionReplicas], version: Int = 3) extends ToJson

object PartitionReassignment{
  def from(topic: String, reassignments : List[PartitionConfiguration]) = {
    val topicPartitionReplicas = reassignments.map(partitionConf => new TopicPartitionReplicas(topic, partitionConf.partitionNum, partitionConf.replicas))
    new PartitionReassignment(topicPartitionReplicas)
  }

  val mapper = new ObjectMapper()
  .registerModule(DefaultScalaModule)
  .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}

case class PartitionConfiguration(partitionNum: Int, replicas: List[Int])

case class TopicConfig(topic:String, partitionCount:Int, replicationFactor:Int, configs: String) extends ToJson

object TopicConfig{
  val Topic = "Topic"
  val PartitionCount = "PartitionCount"
  val ReplicationFactor = "ReplicationFactor"
  val Configs = "Configs"
  def from(properties: Map[String, String]) = {
    new TopicConfig(properties(Topic), properties(PartitionCount).toInt, properties(ReplicationFactor).toInt, properties(Configs))
  }
}