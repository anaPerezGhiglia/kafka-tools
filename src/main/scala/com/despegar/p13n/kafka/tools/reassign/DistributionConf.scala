package com.despegar.p13n.kafka.tools.reassign

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId

case class DistributionConf(maxBrokerLoad: Int, maxLeadership: Int)

object DistributionConf{
  def calculateFrom(brokers: List[BrokerId], replicationFactor: Int, partitions: List[Int]) = {
    new DistributionConf(maxBrokerLoad(brokers, replicationFactor, partitions), maxBrokerLeadership(brokers, partitions))
  }

  protected[tools] def maxBrokerLoad(brokers: List[BrokerId], replicationFactor: Int, partitions: List[Int]) = {
    val amountOfReplicas = partitions.size * replicationFactor
    maxAmountPerBroker(amountOfReplicas, brokers)
  }

  protected[tools] def maxBrokerLeadership(brokers: List[BrokerId], partitions: List[Int]) = {
    maxAmountPerBroker(partitions.size, brokers)
  }

  private def maxAmountPerBroker(toBeDistributed: Int, brokers: List[BrokerId]) = {
    val atLeast = toBeDistributed / brokers.size
    val mod = toBeDistributed % brokers.size
    if (mod == 0) atLeast else atLeast + 1
  }
}