package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId

case class TopicPartition(numOfPartition: Int, leader: Int, replicas: List[Int], isr: List[Int])

object TopicPartition {

  private val PartitionKey = "Partition"
  private val LeaderKey = "Leader"
  private val ReplicasKey = "Replicas"
  private val IsrKey = "Isr"

  def buildFrom(properties: Map[String, String]): TopicPartition = {
    def buildReplicasList(replicas: String) = replicas.split(",").map(_.toInt).toList

    TopicPartition(properties(PartitionKey).toInt, properties(LeaderKey).toInt, buildReplicasList(properties(ReplicasKey)), buildReplicasList(properties(IsrKey)))
  }
}