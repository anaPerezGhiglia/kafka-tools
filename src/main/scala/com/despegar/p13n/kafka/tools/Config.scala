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
  import Utils._

  def copyReassignConf[T](newConfig: (ReassignConfig, T) => ReassignConfig)(value: T, config: Config) = config.copyReassign(newConfig(config.reassignConfig, value))

  def copyLagConf[T](newConfig: (LagConfig, T) => LagConfig)(value: T, config: Config) = config.copyLag(newConfig(config.lagConfig, value))

  head("Despegar.com kafka-tools")

  cmd("reassign")
    .action((value, args) => args.copy(command = Command.GENERATE_PARTITIONS_REASSIGNMENTS))
    .text("Generate partition reassignments")
    .children(

      opt[String]('k', "kafka-dir")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(kafkaDir = value)))
        .text("kafka's directory")
        .required,

      opt[String]('t', "topic")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(topic = value)))
        .text("topic of which want to generate partition reassignments")
        .required,

      opt[String]('z', "zookeeper-path")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(zookeeperPath = value)))
        .text("kafka's zookeeper path")
        .required,

      opt[Seq[Int]]('b', "broker-ids")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(brokerIds = value.toList)))
        .text("Comma separated list of whitelisted brokers to spred replicas across"),

      opt[Seq[Int]]('p', "partitions")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(partitionsToReassign = value.toList)))
        .text("partitions to reassign. If not provided all partitions are reassigned"),

      opt[Int]('r', "replication-factor")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(newReplicationFactor = Some(value))))
        .text("new replication factor. If not provided partitions are reassigned taking into account the topic actual RF"),

      opt[String]('f', "file-name")
        .action(copyReassignConf((reassignConf,value) => reassignConf.copy(fileName = Some(value))))
        .text("fully qualified file name to generate json. If not provided generated in /tmp/reassignPartitions.json")
    )

  cmd("lag")
    .text("Checks lag of determined groupid")
    .action((value, config) => config.copy(command = Command.CHECK_LAG))
    .children(

      opt[String]('k', "kafka-dir")
        .action(copyLagConf((lagConfig, value) => lagConfig.copy(kafkaDir = value)))
        .text("kafka's directory")
        .required,

      opt[String]('g', "group-id")
        .action(copyLagConf((lagConfig, value) => lagConfig.copy(groupid = value)))
        .text("consumer group-id")
        .required(),

      opt[String]('s', "bootstrap-servers")
        .action(copyLagConf((lagConfig, value) => lagConfig.copy(bootstrapServers = value)))
        .text("kafka's bootstrap servers")
        .required(),

      opt[String]('t', "topic")
        .action(copyLagConf((lagConfig, value) => lagConfig.copy(topic = Some(value))))
        .text("topic of which want to evaluate lag. If not provided all topics consumed by the group-id will me evaluated")
    )
}