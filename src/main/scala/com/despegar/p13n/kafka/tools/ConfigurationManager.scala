package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId

object ConfigurationManager {

  def reassignParameters(topicConfig: TopicConfig, actualReplicasAssignments: List[TopicPartition])(implicit config: Config) = {
    println

    val partitionsByBroker = getPartitionsByBroker(actualReplicasAssignments)

    val partitionsToReassign = selectPartitionsToReassign(actualReplicasAssignments)
    val replicationFactor = selectReplicationFactor(topicConfig)
    val brokers: List[BrokerId] = selectBrokers(partitionsByBroker.keys.toList)

    if (brokers.size < replicationFactor) {
      throw new IllegalArgumentException("Valid brokers size does not reach replication factor")
    }

    val distributionConf = DistributionConf.calculateFrom(brokers, replicationFactor, partitionsToReassign)
    println
    println(s"* Each broker must have as much ${distributionConf.maxBrokerLoad} partitions")
    println(s"* Each broker must be as much preferred replica of ${distributionConf.maxLeadership} partitions")

    val brokersLoad: Map[BrokerId, BrokerId] = calculateBrokersLoad(brokers, partitionsByBroker, partitionsToReassign)
    new PartitionsReassignmentsInput(partitionsToReassign, replicationFactor, brokersLoad, distributionConf)
  }

  protected[tools] def getPartitionsByBroker(actualReplicasAssignments: List[TopicPartition]) = {
    actualReplicasAssignments
      .flatMap(partition => partition.replicas.map(replica => (replica, partition)))
      .groupBy(_._1)
      .mapValues(groupped => groupped.map(_._2.numOfPartition))
      .mapValues(partitions => partitions.sorted)
  }

  protected[tools] def selectReplicationFactor(topicConfig: TopicConfig)(implicit config: Config) = {
    val actualReplicationFactor = topicConfig.replicationFactor
    val optNewReplicationFactor = config.newReplicationFactor
    optNewReplicationFactor.foreach(newReplicationFactor => {
      if (newReplicationFactor < 1) {
        throw new IllegalArgumentException("Replication factor must be > 0")
      }

      if (newReplicationFactor < actualReplicationFactor) {
        println(s"Reducing replication factor from $actualReplicationFactor to $newReplicationFactor")
      }
    })

    val replicationFactor = optNewReplicationFactor.getOrElse(actualReplicationFactor)
    println(s"Replication factor: $replicationFactor")
    replicationFactor
  }

  protected[tools] def selectPartitionsToReassign(partitions: List[TopicPartition])(implicit config: Config): List[Int] = {
    val partitionsNumbers = partitions.map(_.numOfPartition)
    val partitionsWithinTopic = config.partitionsToReassign.filter(partitionsNumbers.contains(_))
    if (partitionsWithinTopic.isEmpty) {
      println(s"All ${partitions.size} partitions are going to be reassigned")
      partitionsNumbers
    } else {
      println(s"Partitions to be reassigned: ${config.partitionsToReassign.mkString(",")}")
      partitionsWithinTopic
    }
  }

  protected[tools] def selectBrokers(actualBrokers: List[BrokerId])(implicit config: Config): List[Int] = {
    def subtract(minuend: List[Int], subtrahend: List[Int]) = minuend.filterNot(subtrahend.contains(_)).sorted

    val whitelistedBrokers = config.brokerIds
    if (!whitelistedBrokers.isEmpty) {
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
    }else{
      println(s"No brokers where provided. Using topic's actual brokers: ${actualBrokers.mkString(",")}")
      actualBrokers
    }
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
  def calculateBrokersLoad(brokers: List[BrokerId], partitionsByBroker: Map[BrokerId, List[Int]], partitionsToReassign: List[Int]) = {
    brokers.map(brokerId => {
      val brokerLoad = partitionsByBroker.getOrElse(brokerId, List())
        .filter(partition => partitionsToReassign contains partition) //TODO: check
        .size

      brokerId -> brokerLoad
    }).toMap
  }
}

case class PartitionsReassignmentsInput(partitionsToReassign: List[BrokerId], replicationFactor: BrokerId, brokersLoad: Map[BrokerId, BrokerId], distConf: DistributionConf)
