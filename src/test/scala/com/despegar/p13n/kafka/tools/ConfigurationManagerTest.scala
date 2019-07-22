package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.reassign.{ConfigurationManager, TopicConfig, TopicPartition}
import org.scalatest.FlatSpec

class ConfigurationManagerTest extends FlatSpec{

  "ConfigurationManager#selectReplicationFactor" should "fail if the provided RF < 1" in{

    val topicConfig = new TopicConfig("test", 12, 3, "")
    val reassignParameters = ReassignConfig().copy(newReplicationFactor = Some(0))
    assertThrows[IllegalArgumentException](ConfigurationManager.selectReplicationFactor(topicConfig)(reassignParameters))
  }

  it should "select parameter RF over current RF even if it is smaller" in {
    val topicConfig = new TopicConfig("test", 12, 3, "")
    val reassignParameters = ReassignConfig().copy(newReplicationFactor = Some(2))
    assertResult(2)(ConfigurationManager.selectReplicationFactor(topicConfig)(reassignParameters))
  }

  it should "select parameter RF over current RF" in {
    val topicConfig = new TopicConfig("test", 12, 3, "")
    val reassignParameters = ReassignConfig().copy(newReplicationFactor = Some(4))
    assertResult(4)(ConfigurationManager.selectReplicationFactor(topicConfig)(reassignParameters))
  }

  it should "keep the current RF if no RF was provided" in {
    val topicConfig = new TopicConfig("test", 12, 3, "")
    val reassignParameters = ReassignConfig()
    assertResult(3)(ConfigurationManager.selectReplicationFactor(topicConfig)(reassignParameters))
  }


  "ConfigurationManager#selectPartitionsToReassign" should "select all topic partitions if no partitions where provided" in {
    val reassignParameters = ReassignConfig()
    val partitions: List[TopicPartition] = List(TopicPartition(1, 0, List(0,1,2), List(0,1,2)), TopicPartition(2, 1, List(0,1,2), List(0,1,2)), TopicPartition(3, 2, List(0,1,2), List(0,1,2)))
    assertResult(List(1,2,3))(ConfigurationManager.selectPartitionsToReassign(partitions)(reassignParameters))
  }

  it should "select all topic partitions if none of the given partitions are contained in the topic" in {
    val reassignParameters = ReassignConfig().copy(partitionsToReassign = List(4,5,6))
    val partitions: List[TopicPartition] = List(TopicPartition(1, 0, List(0,1,2), List(0,1,2)), TopicPartition(2, 1, List(0,1,2), List(0,1,2)), TopicPartition(3, 2, List(0,1,2), List(0,1,2)))
    assertResult(List(1,2,3))(ConfigurationManager.selectPartitionsToReassign(partitions)(reassignParameters))
  }

  it should "return the partitions provided when are a sublist of the topic partitions" in {
    val reassignParameters = ReassignConfig().copy(partitionsToReassign = List(1,2))
    val partitions: List[TopicPartition] = List(TopicPartition(1, 0, List(0,1,2), List(0,1,2)), TopicPartition(2, 1, List(0,1,2), List(0,1,2)), TopicPartition(3, 2, List(0,1,2), List(0,1,2)))
    assertResult(List(1,2))(ConfigurationManager.selectPartitionsToReassign(partitions)(reassignParameters))
  }

  it should "return the partitions provideded that are within the topic partitions" in {
    val reassignParameters = ReassignConfig().copy(partitionsToReassign = List(1,2,4))
    val partitions: List[TopicPartition] = List(TopicPartition(1, 0, List(0,1,2), List(0,1,2)), TopicPartition(2, 1, List(0,1,2), List(0,1,2)), TopicPartition(3, 2, List(0,1,2), List(0,1,2)))
    assertResult(List(1,2))(ConfigurationManager.selectPartitionsToReassign(partitions)(reassignParameters))
  }


  "ConfigurationManager#selectBrokers" should "return the brokers provided" in {
    val actualBrokers = List(0,1,2)
    val reassignParameters = ReassignConfig().copy(brokerIds = List(0,1,2,3))
    assertResult(List(0,1,2,3))(ConfigurationManager.selectBrokers(actualBrokers)(reassignParameters))
  }

  it should "return the topic's actual brokers if no brokerIds where provided" in {
    val actualBrokers = List(0,1,2)
    val reassignParameters = ReassignConfig()
    assertResult(actualBrokers)(ConfigurationManager.selectBrokers(actualBrokers)(reassignParameters))
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
  it should "return 0 for a broker that has no partitions contained in partitionsToBeReassigned" in {
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

  "ConfigurationManager#reassignParameters" should "fail if valid brokers is less than RF" in {
    val topicConfig = new TopicConfig("test", 12, 3, "")
    val partitions = List(TopicPartition(1, 0, List(0,1,2), List(0,1,2)), TopicPartition(2, 1, List(0,1,2), List(0,1,2)), TopicPartition(3, 2, List(0,1,2), List(0,1,2)))
    val reassignParameters = ReassignConfig().copy(newReplicationFactor = Option(2), brokerIds = List(1))
    assertThrows[IllegalArgumentException](ConfigurationManager.reassignParameters(topicConfig, partitions)(reassignParameters))
  }

  "ConfigurationManager#getPartitionsByBroker" should "return a list of the partition each broker has" in {
    val partitions = List(
      TopicPartition(1, 1, List(0,1,2), List(0,1,2)),
      TopicPartition(2, 0, List(3,4,5), List(3,4,5)),
      TopicPartition(3, 2, List(0,1,2), List(0,1,2)))

    val expectedPartitionsByBroker = Map(
      0 -> List(1, 3),
      1 -> List(1, 3),
      2 -> List(1, 3),
      3 -> List(2),
      4 -> List(2),
      5 -> List(2))
    assertResult(expectedPartitionsByBroker)(ConfigurationManager.getPartitionsByBroker(partitions))
  }

}
