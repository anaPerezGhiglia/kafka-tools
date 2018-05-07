package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId

object ConfigurationManager {

  def reassignParameters(topicConfig: Map[String, String], actualReplicasAssignments: List[TopicPartition], partitionsByBroker: Map[BrokerId, List[BrokerId]])(implicit config: Config) = {
    println
    val partitionsToReassign = selectPartitionsToReassign(actualReplicasAssignments)
    val replicationFactor = selectReplicationFactor(topicConfig)
    val brokers: List[BrokerId] = selectBrokers(partitionsByBroker)

    if (brokers.size < replicationFactor) {
      throw new RuntimeException("Valid brokers size does not reach replication factor")
    }

    val distributionConf = DistributionConf.calculateFrom(brokers, replicationFactor, partitionsToReassign)
    println
    println(s"* Each broker must have as much ${distributionConf.maxBrokerLoad} partitions")
    println(s"* Each broker must be as much preferred replica of ${distributionConf.maxLeadership} partitions")

    val brokersLoad: Map[BrokerId, BrokerId] = calculateBrokersLoad(brokers, partitionsByBroker, partitionsToReassign)
    new PartitionsReassignmentsInput(partitionsToReassign, replicationFactor, brokersLoad, distributionConf)
  }

  private def selectReplicationFactor(topicConfig: Map[String, String])(implicit config: Config) = {
    val actualReplicationFactor = topicConfig("ReplicationFactor").toInt
    val optNewReplicationFactor = config.newReplicationFactor
    optNewReplicationFactor.foreach(newReplicationFactor => {
      if (newReplicationFactor < 1) {
        throw new RuntimeException("Replication factor must be > 0")
      }

      if (newReplicationFactor < actualReplicationFactor) {
        println(s"Reducing replication factor from $actualReplicationFactor to $newReplicationFactor")
      }
    })

    val replicationFactor = optNewReplicationFactor.getOrElse(actualReplicationFactor)
    println(s"Replication factor: $replicationFactor")
    replicationFactor
  }

  private def selectPartitionsToReassign(partitions: List[TopicPartition])(implicit config: Config): List[Int] = {
    if (config.partitionsToReassign.isEmpty) {
      println(s"All ${partitions.size} partitions are going to be reassigned")
      partitions.map(_.numOfPartition)
    } else {
      println(s"Partitions to be reassigned: ${config.partitionsToReassign.mkString(",")}")
      config.partitionsToReassign
    }
  }

  //TODO: if config.brokerIds is empty consider as whitelisted brokers all the brokers of the cluster or all the brokers of the topic
  private def selectBrokers(partitionsByBroker: Map[Int, List[Int]])(implicit config: Config): List[Int] = {
    def subtract(minuend: List[Int], subtrahend: List[Int]) = minuend.filterNot(subtrahend.contains(_)).toList.sorted

    val actualBrokers = partitionsByBroker.keys.toList
    val whitelistedBrokers = config.brokerIds
    val removedBrokers = subtract(actualBrokers, whitelistedBrokers)
    val newBrokers = subtract(whitelistedBrokers, actualBrokers)
    if (!removedBrokers.isEmpty) {
      println(s"Brokers ${removedBrokers.mkString(",")} will no longer be responsible of ${config.topic}")
    }
    if (!newBrokers.isEmpty) {
      println(s"New brokers for ${config.topic}: ${newBrokers.mkString(",")}")
    }
    println(s"Valid Brokers: ${whitelistedBrokers.mkString(",")}")
    whitelistedBrokers
  }

  /**
    * Calculates each broker load, that is, the amount of partitions that each broker has
    * Only the given partitions to be reassigned count
    *
    * @param brokers
    * @param partitionsByBroker
    * @param partitionsToReassign
    * @return
    */
  protected[tools] def calculateBrokersLoad(brokers: List[BrokerId], partitionsByBroker: Map[BrokerId, List[Int]], partitionsToReassign: List[Int]) = {
    brokers.map(brokerId => {
      val brokerLoad = partitionsByBroker.getOrElse(brokerId, List())
        .filter(partition => partitionsToReassign contains partition) //TODO: check
        .size

      brokerId -> brokerLoad
    }).toMap
  }
}

case class PartitionsReassignmentsInput(partitionsToReassign: List[BrokerId], replicationFactor: BrokerId, brokersLoad: Map[BrokerId, BrokerId], distConf: DistributionConf)
