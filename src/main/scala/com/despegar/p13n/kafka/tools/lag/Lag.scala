package com.despegar.p13n.kafka.tools.lag

import com.despegar.p13n.kafka.tools.{LagConfig, Output}

import scala.util.{Failure, Success, Try}
import sys.process._


object Lag {

  def checkLag(implicit config: LagConfig) = {

    println(s"Groupid: ${config.groupid}")
    println(config.topic.map(t => s"Topic: ${t}").getOrElse("Analyzing all topics consumed by groupid"))

    getLagForConsumerGroup match{
      case Success(consumptions) => analyseLag(consumptions, config.topic)
      case Failure(exception) => println(exception) ; System.exit(1)
    }
  }

  private def getLagForConsumerGroup(implicit config: LagConfig): Try[List[ConsumptionInfo]] = {


    val consumerGroupInfo: String = getKafkaConsumerGroup

    val lines = consumerGroupInfo.split("\n").toList.filterNot(_.isEmpty).drop(1)

    val consumptions = lines.map { line =>
      val parametersList = line.split(" ").filterNot(_.isEmpty).toList
      ConsumptionInfo.fromList(parametersList)
    }

    consumptions.filter(_.isEmpty) match {
      case Nil => Success(consumptions.flatten)
      case _ => Failure(new RuntimeException("There was a problem parsing kafka response, sorry!"))
    }
  }

  private def getKafkaConsumerGroup(implicit config: LagConfig) = {
//    import Output._

    println("\nFetching groupid information from kafka...")

    val consumerGroupInfo: String = s"${config.kafkaDir}/bin/kafka-consumer-groups.sh --bootstrap-server ${config.bootstrapServers} --group ${config.groupid} --describe" !!

//    val consumerGroupInfo = consumerGroup
    consumerGroupInfo
  }


  def analyseLag(consumptions: List[ConsumptionInfo], topic: Option[String]) = {

    def filterTopic(entry: (String, List[ConsumptionInfo])) = topic.map(_.equals(entry._1)).getOrElse(true)

    consumptions.groupBy(_.topic)
      .filter(filterTopic)
      .foreach {
        case (topic, info) => {
          println(s"============")
          println(s"topic: $topic")
          println(s"partitions: ${info.size}")

          val mostDelayedConsumer = info.maxBy(_.lag)
          println(s"total lag: ${info.map(_.lag).sum}")
          println(s"max lag by partition: ")
          println(s"\tpartition: ${mostDelayedConsumer.partition}")
          println(s"\tclientid: ${mostDelayedConsumer.clientId}")
          println(s"\tlag: ${mostDelayedConsumer.lag}")
        }
      }

  }
}
