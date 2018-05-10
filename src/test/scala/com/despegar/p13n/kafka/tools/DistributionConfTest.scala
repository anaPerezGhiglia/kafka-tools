package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.KafkaTools.BrokerId
import org.scalatest.FlatSpec

class DistributionConfTest extends FlatSpec{

  "DistributionConf#maxBrokerLoad" should "return the max amount of partitions that a broker may have" in {
    val partitions = List(1, 2, 3, 4)
    val brokers = List(0, 1, 2)
    val replicationFactor = 3

    val expectedMax = 4 // 12 replicas (partitions * RF) / 3 => all brokers should have 3 partitions
    assertResult(expectedMax)(DistributionConf.maxBrokerLoad(brokers, replicationFactor, partitions))
  }
  it should "consider that some brokers may have one partition more than others" in {
    val partitions = List(1, 2, 3, 4)
    val brokers = List(0, 1, 2, 3, 4)
    val replicationFactor = 3

    val expectedMax = 3 // 2 brokers will have 3 partitions and the remaining 3 will have 2 partitions
    assertResult(expectedMax)(DistributionConf.maxBrokerLoad(brokers, replicationFactor, partitions))
  }

  "DistributionConf#maxBrokerLeadership" should "return the max amount of partitions of which a broker may be the leader" in {
    val brokers: List[BrokerId] = List(0, 1, 2)
    val partitions: List[BrokerId] = List(1, 2, 3)

    val expectedMax = 1 // 3 partitions / 3 brokers => all brokers should be leader of 1 partitions
    assertResult(expectedMax)(DistributionConf.maxBrokerLeadership(brokers, partitions))
  }

  it should "consider that some brokers may be leader of one more partition than others" in {
    val brokers: List[BrokerId] = List(0,1,2)
    val partitions: List[BrokerId] = List(1,2,3,4)

    val expectedMax = 2 // 1 broker should be leader of 2 partitions and the remaining 2 should be leader of 1
    assertResult(expectedMax)(DistributionConf.maxBrokerLeadership(brokers, partitions))
  }

}
