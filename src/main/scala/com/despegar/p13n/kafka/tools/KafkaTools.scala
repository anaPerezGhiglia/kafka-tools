package com.despegar.p13n.kafka.tools

import java.io.File

import org.apache.commons.io.FileUtils

import scala.sys.process._
import scala.util.{Failure, Success, Try}

object KafkaTools extends App {

  type BrokerId = Int

  //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
  println("**************************************")
  println("Kafka generate reassign partition tool")
  println("**************************************\n")

  val parser = new ReassignPartitionsParser
  parser.parse(args, Config()) match {
    case Some(config) => {
      config.command match {
        case Command.NONE => parser.showUsage
        case Command.GENERATE_PARTITIONS_REASSIGNMENTS => generatePartitionsReassignments(config)
      }

    }
    case None => Unit // arguments are bad, error message will have been displayed

  }

  private def generatePartitionsReassignments(implicit config: Config) = {
    val (topicConfig: Map[String, String], actualReplicasAssignments: List[TopicPartition]) = getActualTopicDistribution(config)

    val partitionsByBroker = actualReplicasAssignments
      .flatMap(partition => partition.replicas.map(replica => (replica, partition)))
      .groupBy(_._1)
      .mapValues(groupped => groupped.map(_._2.numOfPartition))
      .mapValues(partitions => partitions.sorted)


    println(s"Topic configuration: ")
    topicConfig.foreach { case (key, value) => println(s"\t- $key: $value") }

    val partitionsReassignmentsInput = ConfigurationManager.reassignParameters(topicConfig, actualReplicasAssignments, partitionsByBroker)
    val reassignment: PartitionReassignment = doPartitionsReassignments(actualReplicasAssignments, partitionsReassignmentsInput)
    writeJsonToFile(reassignment.toJson)

    bye
  }

  private def getActualTopicDistribution(config: Config) = {
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
    val actualReplicasAssignments = partitionLines.map(line => TopicPartition.buildFrom(extractPropertiesFromLine(line)))
    (topicConfig, actualReplicasAssignments)
  }

  private def doPartitionsReassignments(actualReplicasAssignments: List[TopicPartition], reassignPartitionData: PartitionsReassignmentsInput)(implicit config: Config) = {
    val (finalBrokerLoad: Map[BrokerId, BrokerId], replicasAssignments: List[PartitionConfiguration]) = PartitionsReassignmentMaster.reassignPartitions(actualReplicasAssignments, reassignPartitionData)
    val finalAssignments = LeadershipBalancer.distributePreferredReplicas(replicasAssignments)(reassignPartitionData.distConf)

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
    reassignment
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

}

