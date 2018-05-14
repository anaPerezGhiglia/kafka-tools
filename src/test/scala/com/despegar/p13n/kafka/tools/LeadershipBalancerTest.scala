package com.despegar.p13n.kafka.tools

import org.scalatest.FlatSpec

class LeadershipBalancerTest extends FlatSpec {


  "LeadershipBalancer#distributePreferredReplicas" should "select one leader even if the first choices are not valid" in {
    implicit val distributionConf = DistributionConf(3, 1)
    val partitions: List[PartitionConfiguration] = List(
      PartitionConfiguration(0, List(3, 0, 2)),
      PartitionConfiguration(1, List(4, 0, 2)),
      PartitionConfiguration(2, List(5, 0, 2)),
      PartitionConfiguration(3, List(4, 3, 5)),
      PartitionConfiguration(4, List(3, 4, 5))
    )

    val expected = Map(
      0 -> 0,
      1 -> 2,
      2 -> 5,
      3 -> 4,
      4 -> 3
    )
    assertResult(expected)(LeadershipBalancer.distributePreferredReplicas(partitions).map(partition => partition.partitionNum -> partition.replicas.head).toMap)
  }
}
