package com.despegar.p13n.kafka.tools

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class PartitionReassignment(partitions: List[TopicPartitionReplicas], version: Int = 3){
  def toJson = PartitionReassignment.mapper.writeValueAsString(this)
}

object PartitionReassignment{
  def from(topic: String, reassignments : List[PartitionConfiguration]) = {
    val topicPartitionReplicas = reassignments.map(partitionConf => new TopicPartitionReplicas(topic, partitionConf.partitionNum, partitionConf.replicas))
    new PartitionReassignment(topicPartitionReplicas)
  }

  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}
case class TopicPartitionReplicas(topic: String, partition: Int, replicas: List[Int])