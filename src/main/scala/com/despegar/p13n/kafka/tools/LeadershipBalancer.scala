package com.despegar.p13n.kafka.tools


object LeadershipBalancer {

  /**
    * Returns the given partitions replicas sorted to balance preferred replicas
    *
    * @param actualAssignments
    * @param distributionConf
    * @return
    */
  def distributePreferredReplicas(actualAssignments: List[PartitionConfiguration])(implicit distributionConf: DistributionConf) = {
    println
    println("**************************************************")
    println
    println("Sorting replicas to balance preferred replicas...")

    def sort(partitions: List[PartitionConfiguration]) = partitions.sortBy(_.partitionNum)
    var missing = actualAssignments
    var proposedReplicasAssignment: List[PartitionConfiguration] = List()
    var times = 0
    val limit = 10
    do {
      distributeAssignments(missing, proposedReplicasAssignment) match {
        case (proposed, miss) => {
          missing = miss
          proposedReplicasAssignment = proposed
        }
      }
      times = times + 1

    } while (!missing.isEmpty && times < limit)

    if (missing.isEmpty) {
      sort(proposedReplicasAssignment)
    } else {
      println("**** Balanced preferred replica is not assured")
      sort(proposedReplicasAssignment ++ missing)
    }
  }

  protected def distributeAssignments(replicasToDistribute: List[PartitionConfiguration], actualDistribution: List[PartitionConfiguration])(implicit distributionConf: DistributionConf): (List[PartitionConfiguration], List[PartitionConfiguration]) = {
    val proposedReplicasAssignment: List[PartitionConfiguration] = sortReplicasBestEffort(replicasToDistribute, actualDistribution)
    val missingAssignments = replicasToDistribute.map(_.partitionNum).diff(proposedReplicasAssignment.map(_.partitionNum))
    val missingPartitions = replicasToDistribute.filter(assignment => missingAssignments.contains(assignment.partitionNum))
    (proposedReplicasAssignment, missingPartitions)
  }

  /**
    * Tries to sort replicas of all partitions.
    * Does not guarantee that the result contains all the partitions
    *
    * @param replicasAssignments
    * @param alreadySorted
    * @param distributionConf
    * @return
    */
  protected def sortReplicasBestEffort(replicasAssignments: List[PartitionConfiguration], alreadySorted: List[PartitionConfiguration] = List())(implicit distributionConf: DistributionConf) = {
    replicasAssignments.foldLeft(alreadySorted)((acc, elem) => {
      val partitionsByPreferredReplica = acc.groupBy(_.replicas.head)
      val sortedReplicas = elem.replicas.sortBy(brokerId => partitionsByPreferredReplica.get(brokerId).map(_.size).getOrElse(0))

      val result = if (partitionsByPreferredReplica.get(sortedReplicas.head).map(_.size).getOrElse(0) >= distributionConf.maxLeadership) {
        val partitionToInvalidate = partitionsByPreferredReplica(sortedReplicas.head).head
        acc.diff(List(partitionToInvalidate))
      } else {
        acc
      }

      val newPartitionConf = elem.copy(replicas = sortedReplicas)
      result :+ newPartitionConf
    })
  }
}
