import com.beust.jcommander.JCommander
import scopt.OptionParser

import collection.JavaConverters._
import sys.process._

object KafkaTools extends App {

  //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
  println("**************************************")
  println("Kafka generate reassign partition tool")
  println("**************************************\n")

  val parser = new ReassignPartitionsParser
  parser.parse(args, Config()) match {
    case Some(config) => generatePartitionsReassignments(config)
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
      finish
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
    //    partitionsByBroker.foreach { case (broker, partitions) => println(s"Broker $broker has ${partitions.size} partitions: $partitions") }

    println
    val partitions = partitionsToReassign(topicPartitions)
    val replicationFactor = selectReplicationFactor(topicConfig)
    val brokers: List[BrokerId] = selectBrokers(partitionsByBroker)

    if (brokers.size < replicationFactor) {
      throw new RuntimeException("Valid brokers size does not reach replication factor")
    }
    val brokersLoad: Map[BrokerId, Int] = brokers.map(brokerId => brokerId -> partitionsByBroker.get(brokerId).map(_.size).getOrElse(0)).toMap

    val seed = (brokersLoad, List(): List[PartitionConfiguration])
    val (_, replicasAssignments) = partitions.foldLeft(seed){case ((amountOfPartitionsByBroker, partitionsConfigs), partition) => {
      println
      println(s"Partition: $partition")
      val replicas = topicPartitions.filter(_.numOfPartition.equals(partition)).head.replicas
      println(s"Actual replicas: ${replicas.mkString(",")}")

      val reassignment: Reassignment = newReplicasForPartition(replicationFactor, new Reassignment(amountOfPartitionsByBroker, new PartitionConfiguration(partition, replicas)))

      val partitionConfiguration = reassignment.partitionConf
      println(s"Proposed replicas: ${partitionConfiguration.replicas.mkString(",")}")
      (reassignment.brokersLoad, partitionsConfigs :+ partitionConfiguration)
    }}

    println(replicasAssignments)
  }

  def selectReplicationFactor(topicConfig: Map[String, String])(implicit config: Config) = {
    val actualReplicationFactor = topicConfig("ReplicationFactor").toInt
    val optNewReplicationFactor = config.newReplicationFactor
    optNewReplicationFactor.foreach(newReplicationFactor => {
      if (newReplicationFactor < actualReplicationFactor) {
        println(s"Reducing replication factor from $actualReplicationFactor to $newReplicationFactor")
      }
    })

    val replicationFactor = optNewReplicationFactor.getOrElse(actualReplicationFactor)
    println(s"Replication factor: $replicationFactor")
    replicationFactor
  }


  case class Reassignment(brokersLoad: Map[BrokerId, Int], partitionConf: PartitionConfiguration)


  //TODO: if adding new brokers make them leader
  private def newReplicasForPartition(replicationFactor: Int, reassignment: Reassignment) = {
    val partitionConf = reassignment.partitionConf
    // , replicas: List[BrokerId], brokersLoad: Map[BrokerId, Int]): (List[BrokerId], Map[BrokerId, Int]) = {
    val validReplicas = partitionConf.replicas.filter(reassignment.brokersLoad.contains(_))
    if (validReplicas.size >= replicationFactor) {
      val removedReplicas = validReplicas.drop(replicationFactor)
      val newBrokersLoad = reassignment.brokersLoad.map {
        case (brokerId, load) if removedReplicas.contains(brokerId) => (brokerId, load - 1)
        case entry => entry
      }
      val newPartitionConf = partitionConf.copy(replicas = validReplicas.take(replicationFactor))
      reassignment.copy(brokersLoad = newBrokersLoad, partitionConf = newPartitionConf)
    } else {
      //must add a new broker
      val missingBrokers = replicationFactor - validReplicas.size
      val newBrokers = selectLessLoadedBroker(missingBrokers, reassignment.brokersLoad)
      println(s"New brokers for partition: ${newBrokers.mkString(",")}")
      val newBrokersLoad = reassignment.brokersLoad.map {
        case (brokerId, load) if newBrokers.contains(brokerId) => (brokerId, load + 1)
        case entry => entry
      }
      val newPartitionConf = partitionConf.copy(replicas = validReplicas ++ newBrokers)
      reassignment.copy(brokersLoad = newBrokersLoad, partitionConf = newPartitionConf)
    }
  }

  def selectLessLoadedBroker(missingBrokers: Int, brokerLoad: Map[BrokerId, Int]): List[BrokerId] = {
    brokerLoad.toList.sortBy { case (_, amountOfPartitions) => amountOfPartitions }.take(missingBrokers).map(_._1)
  }

  def brokersWithPartition(partition: Int, partitionsByBroker: Map[Int, List[Int]]) = partitionsByBroker.filter {
    case (_, partitions) => partitions.contains(partition)
  }.map {
    case (broker, _) => broker
  }.toList


  def partitionsToReassign(partitions: List[TopicPartition])(implicit config: Config): List[Int] = {
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

  private def finish = {
    println
    println("bye!")
    System.exit(0)
  }
}
