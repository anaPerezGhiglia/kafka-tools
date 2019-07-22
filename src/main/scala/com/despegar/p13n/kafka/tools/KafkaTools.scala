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
        case Command.GENERATE_PARTITIONS_REASSIGNMENTS => generatePartitionsReassignments(config.reassignConfig)
        case Command.CHECK_LAG => checkLag(config.lagConfig)
      }

    }
    case None => Unit // arguments are bad, error message will have been displayed

  }

  private def checkLag(implicit config: LagConfig) = {
    println("LAG CHECK FOR GROUPID")
    println(s"Groupid: ${config.groupid}")
    println(config.topic.map(t => s"Topic: ${t}").getOrElse("Analyzing all topics consumed by groupid"))
    println("\nFetching topic information from kafka...")

    import Output._

    //    val consumerGroupInfo = s"${config.kafkaDir}/bin/kafka-consumer-groups.sh --bootstrap-server ${config.bootstrapServers} --group ${config.groupid} --describe" !!
    val consumerGroupInfo = consumerGroup

    val lines = consumerGroupInfo.split("\n").toList.filterNot(_.isEmpty).drop(1)

    sealed abstract case class ConsumptionInfo(topic: String, partition: Int, currentOffset: Long, longEndOffset: Long, lag: Long, consumerId: String, clientId: String)

    object ConsumptionInfo{
      def parse[T]: (String => T) => String => Option[T] = func => str => Try(func(str)).toOption

      def fromList(list: List[String]): Option[ConsumptionInfo] = {

        Option(list).filter(_.size == 7).flatMap { l =>
           for {
            partition <- parse(_.toInt)(l(1))
            currentOffset <- parse(_.toLong)(l(2))
            longEndOffset <- parse(_.toLong)(l(3))
            lag <- parse(_.toInt)(l(4))
            val topic = l(0)
            val consumerId = l(5)
            val clientId = l(6)
          } yield new ConsumptionInfo(topic, partition, currentOffset, longEndOffset, lag, consumerId, clientId){}
        }
      }
    }

//    val buildConsumptionInfo : List[String] =
    val consumptions = lines.map{ line =>
      val parametersList = line.split(" ").filterNot(_.isEmpty).toList
      ConsumptionInfo.fromList(parametersList)
    }
    consumptions.filter(_.isEmpty) match {
      case Nil =>
        analyseLag(consumptions.flatten, config.topic)
      case _ => println("There was a problem parsing kafka response, sorry!"); System.exit(1)
    }

//    case class Lag(consumer: String, lag: Long)
    def analyseLag(consumptions: List[ConsumptionInfo], topic: Option[String]) = {

      def filterTopic(entry: (String, List[ConsumptionInfo])) =  topic.map(_.equals(entry._1)).getOrElse(true)// topic

      consumptions.groupBy(_.topic)
        .filter(filterTopic)
        .foreach{
        case (topic, info) => {
          println(s"============")
          println(s"topic: $topic")
          println(s"partitions: ${info.size}")
//          val lags = info.map( cons => Lag(cons.clientId, cons.lag))

          val mostDelayedConsumer = info.maxBy(_.lag)
          println(s"total lag: ${info.map(_.lag).sum}")
          println(s"max lag by partition: ")
          println(s"\tpartition: ${mostDelayedConsumer.partition}")
          println(s"\tclientid: ${mostDelayedConsumer.clientId}")
          println(s"\tlag: ${mostDelayedConsumer.lag}")
//          info.sortBy(_.partition).foreach(println)
        }
      }

    }

  }

  private def generatePartitionsReassignments(implicit config: ReassignConfig) = {
    val (topicConfig: TopicConfig, actualReplicasAssignments: List[TopicPartition]) = getActualTopicDistribution(config)

    println(s"Topic configuration: ")
    println(topicConfig.toJson)

    val partitionsReassignmentsInput = ConfigurationManager.reassignParameters(topicConfig, actualReplicasAssignments)
    val reassignment: PartitionReassignment = doPartitionsReassignments(actualReplicasAssignments, partitionsReassignmentsInput)
    writeJsonToFile(reassignment.toJson)

    bye
  }

  private def getActualTopicDistribution(config: ReassignConfig) = {
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

  private def doPartitionsReassignments(actualReplicasAssignments: List[TopicPartition], reassignPartitionData: PartitionsReassignmentsInput)(implicit config: ReassignConfig) = {
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

  private def writeJsonToFile(json: String)(implicit config: ReassignConfig) = {
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

