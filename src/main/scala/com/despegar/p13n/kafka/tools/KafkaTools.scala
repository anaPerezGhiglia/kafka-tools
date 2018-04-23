package com.despegar.p13n.kafka.tools

import java.io.File

import org.apache.commons.io.FileUtils

import scala.sys.process._
import scala.util.{Failure, Success, Try}

object KafkaTools extends App {

  //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
  println("**************************************")
  println("Kafka generate reassign partition tool")
  println("**************************************\n")

  val parser = new ReassignPartitionsParser
  parser.parse(args, Config()) match {
    case Some(config) => {
      config.command match{
        case Command.NONE => parser.showUsage
        case Command.GENERATE_PARTITIONS_REASSIGNMENTS => generatePartitionsReassignments(config)
      }

    }
    case None => Unit // arguments are bad, error message will have been displayed

  }

  type BrokerId = Int

  private def generatePartitionsReassignments(implicit config: Config) = {
    println("Fetching topic information from kafka...")

    //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
    //    val topicDescribe = s"${config.kafkaDir}/bin/kafka-topics.sh --zookeeper ${config.zookeeperPath} --topic ${config.topic} --describe" !!
    val topicDescribe = s"cat topicDescribe.txt" !!

    if (topicDescribe.isEmpty) {
      println(s"No information of topic ${config.topic} in zookeeper ${config.zookeeperPath}")
      bye
    }

    val lines = topicDescribe.split("\n")
    val head = lines.toList.take(1).head
    val partitionLines = lines.toList.drop(1)
    val topicConfig = extractPropertiesFromLine(head)
    val topicPartitions = partitionLines.map(line => TopicPartition.buildFrom(extractPropertiesFromLine(line)))

    val partitionsByBroker = topicPartitions
      .flatMap(partition => partition.replicas.map(replica => (replica, partition)))
      .groupBy(_._1)
      .mapValues(groupped => groupped.map(_._2.numOfPartition))
      .mapValues(partitions => partitions.sorted)


    println(s"Topic configuration: ")
    topicConfig.foreach { case (key, value) => println(s"\t- $key: $value") }

    println
    val partitionsToReassign = selectPartitionsToReassign(topicPartitions)
    val replicationFactor = selectReplicationFactor(topicConfig)
    val brokers: List[BrokerId] = selectBrokers(partitionsByBroker)

    if (brokers.size < replicationFactor) {
      throw new RuntimeException("Valid brokers size does not reach replication factor")
    }

    implicit val distributionConf = DistributionConf.calculateFrom(brokers, replicationFactor, partitionsToReassign)
    println
    println(s"* Each broker must have as much ${distributionConf.maxBrokerLoad} partitions")
    println(s"* Each broker must be as much preferred replica of ${distributionConf.maxLeadership} partitions")

    val brokersLoad: Map[BrokerId, Int] = calculateBrokersLoad(brokers, partitionsByBroker, partitionsToReassign)
    val (finalBrokerLoad: Map[BrokerId, Int], replicasAssignments: List[PartitionConfiguration]) = reassignPartitions(topicPartitions, replicationFactor, partitionsToReassign, brokersLoad)
    val finalAssignments = distributePreferredReplicas(replicasAssignments)

    println
    val leadershipByBroker = finalAssignments.groupBy(_.replicas.head)
    finalBrokerLoad.toList.sortBy { case (brokerId, load) => load }.foreach {
      case (brokerId, load) => println(s"Broker $brokerId has $load partitions and is preferred replica of ${leadershipByBroker(brokerId).size} partitions")
    }
    println
    val sortedAssignments = finalAssignments.sortBy(_.partitionNum)
    sortedAssignments.foreach(partitionConf =>
      println(s"Partition ${partitionConf.partitionNum} replicas are in brokers ${partitionConf.replicas.mkString(",")}")
    )

    val reassignment = PartitionReassignment.from(config.topic, sortedAssignments)
    val json = reassignment.toJson

    writeJsonToFile(json)

    bye
  }

  private def writeJsonToFile(json: String)(implicit config: Config) = {
    println
    println("Writing json to file...")
    val fileName = config.fileName.getOrElse("/tmp/reassignPartitions.json")
    val file = new File(fileName)
    Try(
      FileUtils.write(file, json, "UTF-8")
    ) match {
      case Failure(_) => {
        println(s"Error writing file $fileName")
        println(json)
      }
      case Success(_) => println(s"Reassign parititions file: $fileName")
    }
  }

  private def reassignPartitions(topicPartitions: List[TopicPartition], replicationFactor: Int, partitionsToReassign: List[BrokerId], brokersLoad: Map[BrokerId, Int])(implicit distributionConf: DistributionConf) = {
    val seed = (brokersLoad, List(): List[PartitionConfiguration])
    val (finalBrokerLoad, replicasAssignments) = partitionsToReassign.foldLeft(seed) {
      case ((amountOfPartitionsByBroker, partitionsConfigs), partition) => {
        println
        println("...............")
        println(s"Partition: $partition")
        println("...............")
        val replicas = topicPartitions.filter(_.numOfPartition.equals(partition)).head.replicas
        println(s"Actual replicas: ${replicas.mkString(",")}")

        val reassignment: Reassignment = newReplicasForPartition(replicationFactor, new Reassignment(amountOfPartitionsByBroker, new PartitionConfiguration(partition, replicas)))

        val partitionConfiguration = reassignment.partitionConf
        (reassignment.brokersLoad, partitionsConfigs :+ partitionConfiguration)
      }
    }
    (finalBrokerLoad, replicasAssignments)
  }

  /**
    * Returns the given partitions replicas sorted to balance preferred replicas
    *
    * @param actualAssignments
    * @param distributionConf
    * @return
    */
  def distributePreferredReplicas(actualAssignments: List[PartitionConfiguration])(implicit distributionConf: DistributionConf) = {
    println
    println("**************************************************")
    println
    println("Sorting replicas to balance preferred replicas...")

    var missing = actualAssignments
    var proposedReplicasAssignment: List[PartitionConfiguration] = List()
    var times = 0
    val limit = 10
    do {
      distributeAssignments(missing, proposedReplicasAssignment) match {
        case (proposed, miss) => {
          missing = miss
          proposedReplicasAssignment = proposed
        }
      }
      times = times + 1

    } while (!missing.isEmpty && times < limit)

    if (missing.isEmpty) {
      proposedReplicasAssignment
    } else {
      println("**** Balanced preferred replica is not assured")
      proposedReplicasAssignment ++ missing
    }
  }


  def distributeAssignments(replicasToDistribute: List[PartitionConfiguration], actualDistribution: List[PartitionConfiguration])(implicit distributionConf: DistributionConf): (List[PartitionConfiguration], List[PartitionConfiguration]) = {
    val proposedReplicasAssignment: List[PartitionConfiguration] = sortReplicasBestEffort(replicasToDistribute, actualDistribution)
    val missingAssignments = replicasToDistribute.map(_.partitionNum).diff(proposedReplicasAssignment.map(_.partitionNum))
    val missingPartitions = replicasToDistribute.filter(assignment => missingAssignments.contains(assignment.partitionNum))
    (proposedReplicasAssignment, missingPartitions)
  }

  /**
    * Tries to sort replicas of all partitions.
    * Does not guarantee that the result contains all the partitions
    *
    * @param replicasAssignments
    * @param alreadySorted
    * @param distributionConf
    * @return
    */
  def sortReplicasBestEffort(replicasAssignments: List[PartitionConfiguration], alreadySorted: List[PartitionConfiguration] = List())(implicit distributionConf: DistributionConf) = {
    replicasAssignments.foldLeft(alreadySorted)((acc, elem) => {
      val partitionsByPreferredReplica = acc.groupBy(_.replicas.head)
      val sortedReplicas = elem.replicas.sortBy(brokerId => partitionsByPreferredReplica.get(brokerId).map(_.size).getOrElse(0))

      val result = if (partitionsByPreferredReplica.get(sortedReplicas.head).map(_.size).getOrElse(0) >= distributionConf.maxLeadership) {
        val partitionToInvalidate = partitionsByPreferredReplica(sortedReplicas.head).head
        acc.diff(List(partitionToInvalidate))
      } else {
        acc
      }

      val newPartitionConf = elem.copy(replicas = sortedReplicas)
      result :+ newPartitionConf
    })
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

  def selectReplicationFactor(topicConfig: Map[String, String])(implicit config: Config) = {
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

  def maxAmountPerBroker(toBeDistributed: Int, brokers: List[BrokerId]) = {
    val atLeast = toBeDistributed / brokers.size
    val mod = toBeDistributed % brokers.size
    if (mod == 0) atLeast else atLeast + 1
  }


  private def newReplicasForPartition(replicationFactor: Int, reassignment: Reassignment)(implicit distributionConf: DistributionConf) = {
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
    generateNewReassignment(replicationFactor, applicableReplicas, updatedReassignment, newReassignmentFunc)
  }

  private def generateNewReassignment(replicationFactor: Int, applicableReplicas: List[BrokerId], actualReassignment: Reassignment, newReassignment: (List[BrokerId], Int => Int, List[BrokerId]) => Reassignment) = {
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

  def configureNewReassignment(reassignment: Reassignment)(affectedBrokers: List[BrokerId], loadOperation: Int => Int, proposedReplicas: List[BrokerId]): Reassignment = {
    val actualBrokersLoad = newBrokersLoad(reassignment.brokersLoad, affectedBrokers, loadOperation)
    val newPartitionConf = reassignment.partitionConf.copy(replicas = proposedReplicas)
    reassignment.copy(brokersLoad = actualBrokersLoad, partitionConf = newPartitionConf)
  }

  def selectLessLoadedBrokers(missingBrokers: Int, brokerLoad: Map[BrokerId, Int], excludeBrokers: List[Int]): List[BrokerId] = {
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

  def brokersWithPartition(partition: Int, partitionsByBroker: Map[Int, List[Int]]) = partitionsByBroker.filter {
    case (_, partitions) => partitions.contains(partition)
  }.map {
    case (broker, _) => broker
  }.toList


  def newBrokersLoad(actual: Map[BrokerId, Int], affectedBrokers: List[BrokerId], operation: Int => Int): Map[BrokerId, Int] = {
    actual.map {
      case (brokerId, load) if affectedBrokers.contains(brokerId) => (brokerId, operation(load))
      case entry => entry
    }
  }

  def selectPartitionsToReassign(partitions: List[TopicPartition])(implicit config: Config): List[Int] = {
    if (config.partitionsToReassign.isEmpty) {
      println(s"All ${partitions.size} partitions are going to be reassigned")
      partitions.map(_.numOfPartition)
    } else {
      println(s"Partitions to be reassigned: ${config.partitionsToReassign.mkString(",")}")
      config.partitionsToReassign
    }
  }

  //TODO: if config.brokerIds is empty consider as whitelisted brokers all the brokers of the cluster or all the brokers of the topic
  def selectBrokers(partitionsByBroker: Map[Int, List[Int]])(implicit config: Config): List[Int] = {
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

  protected def extractPropertiesFromLine(line: String): Map[String, String] = {
    line.split("\t").filterNot(_.isEmpty).map(conf => {
      val keyValue = conf.split(":")
      val key = keyValue(0).trim
      if (keyValue.size > 1) (key, keyValue(1).trim) else (key, "")
    }).toMap
  }

  private def bye = {
    println
    println("bye!")
    System.exit(0)
  }

  case class Reassignment(brokersLoad: Map[BrokerId, Int], partitionConf: PartitionConfiguration)

}

