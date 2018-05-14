package com.despegar.p13n.kafka.tools

import scopt.OptionParser

case class Config(brokerIds: List[Int] = List(),
                  topic: String = "",
                  zookeeperPath: String = "",
                  kafkaDir: String = "",
                  partitionsToReassign: List[Int] = List(),
                  newReplicationFactor: Option[Int] = None,
                  fileName: Option[String] = None,
                  command: Command = Command.NONE) {
}


class ReassignPartitionsParser extends OptionParser[Config]("kafka-tools") {
  head("Despegar.com kafka-tools")

  cmd("reassign")
    .action((value, args) => args.copy(command = Command.GENERATE_PARTITIONS_REASSIGNMENTS))
    .text("Generate partition reassignments")
    .children(

      opt[String]('t', "topic")
        .action((value, args) => args.copy(topic = value))
        .text("topic of which want to generate partition reassignments")
        .required,

      opt[String]('z', "zookeeper-path")
        .action((value, args) => args.copy(zookeeperPath = value))
        .text("kafka's zookeeper path")
        .required,

      opt[String]('k', "kafka-dir")
        .action((value, args) => args.copy(kafkaDir = value))
        .text("kafka's directory")
        .required,

      opt[Seq[Int]]('b', "broker-ids")
        .action((value, args) => args.copy(brokerIds = value.toList))
        .text("Comma separated list of whitelisted brokers to spred replicas across"),

      opt[Seq[Int]]('p', "partitions")
        .action((value, args) => args.copy(partitionsToReassign = value.toList))
        .text("partitions to reassign. If not provided all partitions are reassigned"),

      opt[Int]('r', "replication-factor")
        .action((value, args) => args.copy(newReplicationFactor = Some(value)))
        .text("new replication factor. If not provided partitions are reassigned taking into account the topic actual RF"),

      opt[String]('f', "file-name")
        .action((value, args) => args.copy(fileName = Some(value)))
        .text("fully qualified file name to generate json. If not provided generated in /tmp/reassignPartitions.json")
    )

  cmd("lag").text("Checks lag of determined groupid").action((_, config) => config.copy(command = Command.CHECK_LAG))
}