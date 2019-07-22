package com.despegar.p13n.kafka.tools

import scopt.OptionParser

case class ReassignConfig(brokerIds: List[Int] = List(),
                          topic: String = "",
                          zookeeperPath: String = "",
                          kafkaDir: String = "",
                          partitionsToReassign: List[Int] = List(),
                          newReplicationFactor: Option[Int] = None,
                          fileName: Option[String] = None)

case class LagConfig(topic: Option[String] = None,
                     zookeeperPath: String = "",
                     kafkaDir: String = "",
                     bootstrapServers: String = "",
                     groupid: String = "")

case class Config(reassignConfig: ReassignConfig = new ReassignConfig(),
                  lagConfig: LagConfig = new LagConfig(),
                  command: Command = Command.NONE) {
}


class ReassignPartitionsParser extends OptionParser[Config]("kafka-tools") {
  head("Despegar.com kafka-tools")

  def copyReassignConf(config: Config, newConfig: ReassignConfig => ReassignConfig) = config.copy(reassignConfig = newConfig(config.reassignConfig))
  def copyLagConf(config: Config, newConfig: LagConfig => LagConfig) = config.copy(lagConfig = newConfig(config.lagConfig))


  cmd("reassign")
    .action((value, args) => args.copy(command = Command.GENERATE_PARTITIONS_REASSIGNMENTS))
    .text("Generate partition reassignments")
    .children(

      opt[String]('k', "kafka-dir")
        .action((value, args) => copyReassignConf(args, _.copy(kafkaDir = value)))
        .text("kafka's directory")
        .required,

      opt[String]('t', "topic")
        .action((value, args) => copyReassignConf(args, _.copy(topic = value)))
        .text("topic of which want to generate partition reassignments")
        .required,

      opt[String]('z', "zookeeper-path")
        .action((value, args) => copyReassignConf(args, _.copy(zookeeperPath = value)))
        .text("kafka's zookeeper path")
        .required,

      opt[Seq[Int]]('b', "broker-ids")
        .action((value, args) => copyReassignConf(args, _.copy(brokerIds = value.toList)))
        .text("Comma separated list of whitelisted brokers to spred replicas across"),

      opt[Seq[Int]]('p', "partitions")
        .action((value, args) => copyReassignConf(args, _.copy(partitionsToReassign = value.toList)))
        .text("partitions to reassign. If not provided all partitions are reassigned"),

      opt[Int]('r', "replication-factor")
        .action((value, args) => copyReassignConf(args, _.copy(newReplicationFactor = Some(value))))
        .text("new replication factor. If not provided partitions are reassigned taking into account the topic actual RF"),

      opt[String]('f', "file-name")
        .action((value, args) => copyReassignConf(args, _.copy(fileName = Some(value))))
        .text("fully qualified file name to generate json. If not provided generated in /tmp/reassignPartitions.json")
    )

  cmd("lag")
    .text("Checks lag of determined groupid")
    .action((value, config) => config.copy(command = Command.CHECK_LAG))
    .children(

      opt[String]('k', "kafka-dir")
        .action((value, args) => copyLagConf(args, _.copy(kafkaDir = value)))
        .text("kafka's directory")
        .required,

      opt[String]('g', "group-id")
        .action((value, args) => copyLagConf(args, _.copy(groupid = value)))
        .text("consumer group-id")
        .required(),

      opt[String]('s', "bootstrap-servers")
        .action((value, args) => copyLagConf(args, _.copy(bootstrapServers = value)))
        .text("kafka's bootstrap servers")
        .required(),

      opt[String]('t', "topic")
        .action((value, args) => copyLagConf(args, _.copy(topic = Some(value))))
        .text("topic of which want to evaluate lag. If not provided all topics consumed by the group-id will me evaluated")
    )
}