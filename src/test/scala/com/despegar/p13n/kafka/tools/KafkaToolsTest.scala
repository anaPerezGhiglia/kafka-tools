package com.despegar.p13n.kafka.tools

import org.scalatest.FlatSpec

class KafkaToolsTest extends FlatSpec {

  "PartitionsReassignmentMaster#newBrokersLoad" should "change load only of the affectedBrokers" in {
    val brokersLoad = Map(1 -> 3, 2 -> 4, 3 -> 3, 4 -> 5)
    val affectedBrokers = List(2, 4)

    val newBrokersLoad = PartitionsReassignmentMaster.newBrokersLoad(brokersLoad, affectedBrokers, originalLoad => originalLoad - 1)
    assertResult(Map(1 -> 3, 2 -> 3, 3 -> 3, 4 -> 4))(newBrokersLoad)
  }

}
