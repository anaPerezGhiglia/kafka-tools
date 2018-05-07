package com.despegar.p13n.kafka.tools

import org.scalatest.FlatSpec

class KafkaToolsTest extends FlatSpec {

  "PartitionsReassignmentMaster#newBrokersLoad" should "change load only of the affectedBrokers" in {
    val brokersLoad = Map(1 -> 3, 2 -> 4, 3 -> 3, 4 -> 5)
    val affectedBrokers = List(2, 4)

    val newBrokersLoad = PartitionsReassignmentMaster.newBrokersLoad(brokersLoad, affectedBrokers, originalLoad => originalLoad - 1)
    assertResult(Map(1 -> 3, 2 -> 3, 3 -> 3, 4 -> 4))(newBrokersLoad)
  }

  "ConfigurationManager#brokersLoad" should "only take into cosideration the load of partitions to be reassigned" in {
    val brokers = List(1,2,3)
    val partitionsByBroker = Map(1 -> List(1,3,5,7,9,11,13),
      2-> List(2,6,10,14),
      3-> List(0,4,8,12))

    val partitionsToBeReassigned = List.range(0,13) // 0,1,..,12

    val brokersLoad = ConfigurationManager.calculateBrokersLoad(brokers, partitionsByBroker, partitionsToBeReassigned)
    assertResult(Map(1-> 6, 2->3, 3-> 4))(brokersLoad)
  }
  it should "return 0 for a broker that has no partition contained int partitionsToBeReassigned" in {
    val brokers = List(1,2,3)
    val partitionsByBroker = Map(1 -> List(1,3,5,7,9,11,13),
      2-> List(2,6,10,14),
      3-> List(0,4,8,12))

    val partitionsToBeReassigned = List(0,1,3,4,5)

    val brokersLoad = ConfigurationManager.calculateBrokersLoad(brokers, partitionsByBroker, partitionsToBeReassigned)
    assertResult(Map(1-> 3, 2->0, 3-> 2))(brokersLoad)
  }
  it should "return 0 for a broker that is not in partitionsByBroker" in {
    val brokers = List(1,2,3,4)
    val partitionsByBroker = Map(1 -> List(1,3,5,7,9,11,13),
      2-> List(2,6,10,14),
      3-> List(0,4,8,12))

    val partitionsToBeReassigned = List.range(0,10)

    val brokersLoad = ConfigurationManager.calculateBrokersLoad(brokers, partitionsByBroker, partitionsToBeReassigned)
    assertResult(0)(brokersLoad(4))
  }

  it should "not contain a broker that is in partitionsByBroker but not in brokers list" in {
    val brokers = List(1,2,3,4)
    val partitionsByBroker = Map(1 -> List(1,3,5,7,9,11,13),
      2-> List(2,6,10,14),
      3-> List(0,4,8,12),
      5-> List(15,16,17,18))

    val partitionsToBeReassigned = List.range(0,10)

    val brokersLoad = ConfigurationManager.calculateBrokersLoad(brokers, partitionsByBroker, partitionsToBeReassigned)
    assertResult(Set(1,2,3,4))(brokersLoad.keys.toSet)
  }

}
