package com.despegar.p13n.kafka.tools

import java.io.File

import org.apache.commons.io.FileUtils

import scala.sys.process._
import scala.util.{Failure, Success, Try}

object KafkaTools extends App {

  type BrokerId = Int

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
    val (topicConfig: TopicConfig, actualReplicasAssignments: List[TopicPartition]) = getActualTopicDistribution(config)

    println(s"Topic configuration: ")
    println(topicConfig.toJson)

    val partitionsReassignmentsInput = ConfigurationManager.reassignParameters(topicConfig, actualReplicasAssignments)
    val reassignment: PartitionReassignment = doPartitionsReassignments(actualReplicasAssignments, partitionsReassignmentsInput)
    writeJsonToFile(reassignment.toJson)

    bye
  }

  private def getActualTopicDistribution(config: Config) = {
    println("Fetching topic information from kafka...")

    val topicDescribe = s"${config.kafkaDir}/bin/kafka-topics.sh --zookeeper ${config.zookeeperPath} --topic ${config.topic} --describe" !!

    if (topicDescribe.isEmpty) {
      println(s"No information of topic ${config.topic} in zookeeper ${config.zookeeperPath}")
      bye
    }

    val lines = topicDescribe.split("\n")
    val head = lines.toList.take(1).head
    val partitionLines = lines.toList.drop(1)
    val topicConfig = TopicConfig.from(extractPropertiesFromLine(head))
    val actualReplicasAssignments = partitionLines.map(line => TopicPartition.buildFrom(extractPropertiesFromLine(line)))
    (topicConfig, actualReplicasAssignments)
  }

  private def doPartitionsReassignments(actualReplicasAssignments: List[TopicPartition], reassignPartitionData: PartitionsReassignmentsInput)(implicit config: Config) = {
    val (finalBrokerLoad: Map[BrokerId, BrokerId], replicasAssignments: List[PartitionConfiguration]) = PartitionsReassignmentMaster.reassignPartitions(actualReplicasAssignments, reassignPartitionData)
    val finalAssignments = LeadershipBalancer.distributePreferredReplicas(replicasAssignments)(reassignPartitionData.distConf)

    println
    val leadershipByBroker = finalAssignments.groupBy(_.replicas.head)
    finalBrokerLoad.toList.sortBy { case (brokerId, _) => brokerId }.foreach {
      case (brokerId, load) => println(s"Broker $brokerId has $load partitions and is preferred replica of ${leadershipByBroker.get(brokerId).map(_.size).getOrElse(0)} partitions")
    }
    println
    finalAssignments.foreach(partitionConf =>
      println(s"Partition ${partitionConf.partitionNum} replicas are in brokers ${partitionConf.replicas.mkString(",")}")
    )

    val reassignment = PartitionReassignment.from(config.topic, finalAssignments)
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

