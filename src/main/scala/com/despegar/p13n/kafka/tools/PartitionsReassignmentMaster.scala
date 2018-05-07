package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId

object PartitionsReassignmentMaster {

  def reassignPartitions(actualReplicasAssignments: List[TopicPartition], reasignPartitionData: PartitionsReassignmentsInput) = {
    val initial = (reasignPartitionData.brokersLoad, List(): List[PartitionConfiguration])
    val (finalBrokerLoad, replicasAssignments) = reasignPartitionData.partitionsToReassign
      .foldLeft(initial) {
        case ((amountOfPartitionsByBroker, partitionsConfigs), partition) => {

          val reassignment: Assignment = reassignReplicasForPartition(partition, actualReplicasAssignments, reasignPartitionData, amountOfPartitionsByBroker)
          val partitionConfiguration = reassignment.partitionConf

          (reassignment.brokersLoad, partitionsConfigs :+ partitionConfiguration)
        }
      }
    (finalBrokerLoad, replicasAssignments)
  }

  private def reassignReplicasForPartition(partition: Int, actualReplicasAssignments: List[TopicPartition], reasignPartitionData: PartitionsReassignmentsInput, amountOfPartitionsByBroker: Map[BrokerId, Int]) = {
    println
    println("...............")
    println(s"Partition: $partition")
    println("...............")
    val replicas = actualReplicasAssignments.filter(_.numOfPartition.equals(partition)).head.replicas
    println(s"Actual replicas: ${replicas.mkString(",")}")

    val actualAssignment = new Assignment(amountOfPartitionsByBroker, new PartitionConfiguration(partition, replicas))
    val reassignment: Assignment = newReplicasForPartition(reasignPartitionData.replicationFactor, actualAssignment, reasignPartitionData.distConf)
    reassignment
  }

  private def newReplicasForPartition(replicationFactor: Int, reassignment: Assignment, distributionConf: DistributionConf) = {
    val actualPartitionConf = reassignment.partitionConf
    //validReplicas = Partition replicas that belong to whitelisted brokers
    val validReplicas = actualPartitionConf.replicas.filter(reassignment.brokersLoad.contains(_))
    //applicableReplicas = Partitions replicas that are to keep since its load is less or equal than limit (equal because the actual partition is considered in the load)
    val applicableReplicas = validReplicas.filter(replica => reassignment.brokersLoad(replica) <= distributionConf.maxBrokerLoad)
    //ignoredValidReplicas = Partitions replicas that should be removed from this partition assignment since its load is higher than the admitted one
    val ignoredValidReplicas = validReplicas.filterNot(applicableReplicas.contains(_))

    val updatedReassignment = {
      if (ignoredValidReplicas.isEmpty) {
        reassignment
      } else {
        //Since we want to removed the ignoredReplicas from the partition, their load must be decreased in one
        ignoredValidReplicas.foreach(ignoredReplica =>
          println(s"Ignoring replica $ignoredReplica since has ${reassignment.brokersLoad(ignoredReplica)} partitions when the limit is ${distributionConf.maxBrokerLoad}")
        )
        configureNewReassignment(reassignment)(ignoredValidReplicas, originalLoad => originalLoad - 1, validReplicas)
      }
    }
    val newReassignmentFunc = configureNewReassignment(updatedReassignment) _
    generateNewAssignment(replicationFactor, applicableReplicas, updatedReassignment, newReassignmentFunc)
  }

  private def generateNewAssignment(replicationFactor: Int, applicableReplicas: List[BrokerId], actualReassignment: Assignment, newReassignment: (List[BrokerId], Int => Int, List[BrokerId]) => Assignment) = {
    if (applicableReplicas.size >= replicationFactor) {
      val removedReplicas = applicableReplicas.drop(replicationFactor) //In case that partition had more replicas than RF
      val proposedReplicas = applicableReplicas.take(replicationFactor)
      println(s"* keeping original replicas --> ${proposedReplicas.mkString(",")}")

      newReassignment(removedReplicas, originalLoad => originalLoad - 1, proposedReplicas)
    } else {
      //must add a new broker
      val missingBrokers = replicationFactor - applicableReplicas.size
      println(s"Keeping replicas: ${applicableReplicas.mkString(",")}")

      val newBrokers = selectLessLoadedBrokers(missingBrokers, actualReassignment.brokersLoad, applicableReplicas)
      println(s"Selected brokers for partition: ${newBrokers.mkString(",")}")

      val proposedReplicas = applicableReplicas ++ newBrokers
      println
      println(s"* Proposed replicas ---------> ${proposedReplicas.mkString(",")}")

      newReassignment(newBrokers, originalLoad => originalLoad + 1, proposedReplicas)
    }
  }

  private def configureNewReassignment(reassignment: Assignment)(affectedBrokers: List[BrokerId], loadOperation: Int => Int, proposedReplicas: List[BrokerId]): Assignment = {
    val actualBrokersLoad = newBrokersLoad(reassignment.brokersLoad, affectedBrokers, loadOperation)
    val newPartitionConf = reassignment.partitionConf.copy(replicas = proposedReplicas)
    reassignment.copy(brokersLoad = actualBrokersLoad, partitionConf = newPartitionConf)
  }

  private def selectLessLoadedBrokers(missingBrokers: Int, brokerLoad: Map[BrokerId, Int], excludeBrokers: List[Int]): List[BrokerId] = {
    val whitelistedBrokers = brokerLoad.toList
      .filterNot { case (brokerId, _) => excludeBrokers.contains(brokerId) }
      .sortBy { case (_, amountOfPartitions) => amountOfPartitions }
    whitelistedBrokers.foreach {
      case (brokerId, load) => println(s"Broker $brokerId has $load partitions")
    }
    whitelistedBrokers
      .take(missingBrokers)
      .map(_._1)
  }

  def newBrokersLoad(actual: Map[BrokerId, Int], affectedBrokers: List[BrokerId], operation: Int => Int): Map[BrokerId, Int] = {
    actual.map {
      case (brokerId, load) if affectedBrokers.contains(brokerId) => (brokerId, operation(load))
      case entry => entry
    }
  }
}

case class Assignment(brokersLoad: Map[BrokerId, Int], partitionConf: PartitionConfiguration)
