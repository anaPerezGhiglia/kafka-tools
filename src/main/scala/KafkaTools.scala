import scala.sys.process._

object KafkaTools extends App {

  //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
  println("**************************************")
  println("Kafka generate reassign partition tool")
  println("**************************************\n")

  val parser = new ReassignPartitionsParser
  parser.parse(args, Config()) match {
    case Some(config) => generatePartitionsReassignments(config)
    case None => Unit // arguments are bad, error message will have been displayed

  }


  type BrokerId = Int

  private def generatePartitionsReassignments(implicit config: Config) = {
    println("Fetching topic information from kafka...")

    //--zookeeper-path zk-p13n-bsas-rc-01.servers.despegar.it/p13n-kafka --topic upaEvents --broker-ids 0,1,2 --kafka-dir /home/anaperezghiglia/opt/kafka_2.12-1.1.0
    //    val topicDescribe = s"${config.kafkaDir}/bin/kafka-topics.sh --zookeeper ${config.zookeeperPath} --topic ${config.topic} --describe" !!
    val topicDescribe = s"cat topicDescribe.txt" !!

    if (topicDescribe.isEmpty) {
      println(s"No information of topic ${config.topic} in zookeeper ${config.zookeeperPath}")
      finish
    }

    val lines = topicDescribe.split("\n")
    val head = lines.toList.take(1).head
    val partitionLines = lines.toList.drop(1)
    val topicConfig = extractPropertiesFromLine(head)
    val topicPartitions = partitionLines.map(line => TopicPartition.buildFrom(extractPropertiesFromLine(line)))

    val partitionsByBroker = topicPartitions
      .flatMap(partition => partition.replicas.map(replica => (replica, partition)))
      .groupBy(_._1)
      .mapValues(groupped => groupped.map(_._2.numOfPartition))
      .mapValues(partitions => partitions.sorted)


    println(s"Topic configuration: ")
    topicConfig.foreach { case (key, value) => println(s"\t- $key: $value") }
    //    partitionsByBroker.foreach { case (broker, partitions) => println(s"Broker $broker has ${partitions.size} partitions: $partitions") }

    println
    val partitionsToReassign = selectPartitionsToReassign(topicPartitions)
    val replicationFactor = selectReplicationFactor(topicConfig)
    val brokers: List[BrokerId] = selectBrokers(partitionsByBroker)

    if (brokers.size < replicationFactor) {
      throw new RuntimeException("Valid brokers size does not reach replication factor")
    }

    implicit val distributionConf = DistributionConf.calculateFrom(brokers, replicationFactor, partitionsToReassign)
    println(s"Each broker must have as much ${distributionConf.maxBrokerLoad} partitions")
    println(s"Each broker must be as much preferred replica of ${distributionConf.maxLeadership} partitions")

    val brokersLoad: Map[BrokerId, BrokerId] = calculateBrokersLoad(brokers, partitionsByBroker, partitionsToReassign)

    val seed = (brokersLoad, List(): List[PartitionConfiguration])
    val (finalBrokerLoad, replicasAssignments) = partitionsToReassign.foldLeft(seed) {
      case ((amountOfPartitionsByBroker, partitionsConfigs), partition) => {
        println
        println(s"Partition: $partition")
        val replicas = topicPartitions.filter(_.numOfPartition.equals(partition)).head.replicas
        println(s"Actual replicas: ${replicas.mkString(",")}")

        val reassignment: Reassignment = newReplicasForPartition(replicationFactor, new Reassignment(amountOfPartitionsByBroker, new PartitionConfiguration(partition, replicas)))

        val partitionConfiguration = reassignment.partitionConf
        (reassignment.brokersLoad, partitionsConfigs :+ partitionConfiguration)
      }
    }

    println
    println("*******************")
    println("Final configuration")
    println
    finalBrokerLoad.toList.sortBy{case (brokerId, load) => load}.foreach{
      case (brokerId, load) => println(s"Broker $brokerId has $load partitions")
    }
    println
    replicasAssignments.toList.groupBy(_.replicas.head).toList.sortBy{case (_, partitions) => partitions.size}.foreach{
      case (leader, partitions) => println(s"Broker $leader is preferred replica of ${partitions.size} partitions")
    }
    println
    replicasAssignments.foreach(partitionConf =>
        println(s"Partition ${partitionConf.partitionNum} replicas are in brokers ${partitionConf.replicas.mkString(",")}")
    )
  }

  /**
    * Calculates each broker load , that is, the amount of partitions that each broker has
    * Only the given partitions to be reassigned count
    * @param brokers
    * @param partitionsByBroker
    * @param partitionsToReassign
    * @return
    */
  def calculateBrokersLoad(brokers: List[BrokerId], partitionsByBroker: Map[BrokerId, List[Int]], partitionsToReassign: List[Int]) = {
    brokers.map(brokerId => {
      val brokerLoad = partitionsByBroker.getOrElse(brokerId, List())
        .filter(partition => partitionsToReassign contains partition) //TODO: check
        .size

      brokerId -> brokerLoad
    }).toMap
  }

  def selectReplicationFactor(topicConfig: Map[String, String])(implicit config: Config) = {
    val actualReplicationFactor = topicConfig("ReplicationFactor").toInt
    val optNewReplicationFactor = config.newReplicationFactor
    optNewReplicationFactor.foreach(newReplicationFactor => {
      if (newReplicationFactor < 1) {
        throw new RuntimeException("Replication factor must be > 0")
      }

      if (newReplicationFactor < actualReplicationFactor) {
        println(s"Reducing replication factor from $actualReplicationFactor to $newReplicationFactor")
      }
    })

    val replicationFactor = optNewReplicationFactor.getOrElse(actualReplicationFactor)
    println(s"Replication factor: $replicationFactor")
    replicationFactor
  }

  def maxAmountPerBroker(toBeDistributed : Int, brokers: List[BrokerId]) = {
    val atLeast = toBeDistributed / brokers.size
    val mod = toBeDistributed % brokers.size
    if(mod == 0) atLeast else atLeast + 1
  }


  //TODO: if adding new brokers make them leader
  private def newReplicasForPartition(replicationFactor: Int, reassignment: Reassignment)(implicit distributionConf: DistributionConf) = {
    val actualPartitionConf = reassignment.partitionConf
    val validReplicas = actualPartitionConf.replicas
      .filter(reassignment.brokersLoad.contains(_))
    val applicableReplicas = validReplicas.filter(replica => reassignment.brokersLoad(replica) <= distributionConf.maxBrokerLoad)
    val ignoredValidReplicas = validReplicas.filterNot(applicableReplicas.contains(_))

    println(s"Ignoring actual replicas ${ignoredValidReplicas.mkString(",")} since have ${ignoredValidReplicas.map(r => reassignment.brokersLoad(r)).mkString(",")} replicas")
    println(s"Replicas to decrease load: ${ignoredValidReplicas.mkString(",")}")

    val updatedReassignment = configureNewReassignment(reassignment)(ignoredValidReplicas, originalLoad => originalLoad -1)
    val newReassignment = configureNewReassignment(updatedReassignment) _

    if (applicableReplicas.size >= replicationFactor) {
      val removedReplicas = applicableReplicas.drop(replicationFactor) //In case that partition had more replicas than RF
      val proposedReplicas = applicableReplicas.take(replicationFactor)
      println(s"keeping original replicas: ${proposedReplicas.mkString(",")}")

      newReassignment(removedReplicas, originalLoad => originalLoad - 1, proposedReplicas)
    } else {
      //must add a new broker
      val missingBrokers = replicationFactor - applicableReplicas.size
      println(s"Keeping replicas: ${applicableReplicas.mkString(",")}")

      val newBrokers = selectLessLoadedBrokers(missingBrokers, updatedReassignment.brokersLoad, applicableReplicas)
      println(s"New brokers for partition: ${newBrokers.mkString(",")}")

      val proposedReplicas = applicableReplicas ++ newBrokers
      println(s"Proposed replicas: ${proposedReplicas.mkString(",")}")

      newReassignment(newBrokers, originalLoad => originalLoad + 1, proposedReplicas)
    }
  }

  def configureNewReassignment(reassignment: Reassignment)(affectedBrokers: List[BrokerId], loadOperation: Int => Int, proposedReplicas: List[BrokerId] = List()): Reassignment = {
    val actualBrokersLoad = newBrokersLoad(reassignment.brokersLoad, affectedBrokers, loadOperation)
    if(proposedReplicas.isEmpty){
      reassignment.copy(brokersLoad = actualBrokersLoad)
    }else{
      val newPartitionConf = reassignment.partitionConf.copy(replicas = proposedReplicas)
      reassignment.copy(brokersLoad = actualBrokersLoad, partitionConf = newPartitionConf)
    }
  }

  def selectLessLoadedBrokers(missingBrokers: Int, brokerLoad: Map[BrokerId, Int], excludeBrokers: List[Int]): List[BrokerId] = {
    val whitelistedBrokers = brokerLoad.toList
      .filterNot { case (brokerId, _) => excludeBrokers.contains(brokerId) }
      .sortBy { case (_, amountOfPartitions) => amountOfPartitions }
    whitelistedBrokers.foreach{
      case (brokerId, load) => println(s"Broker $brokerId has $load partitions")
    }
    whitelistedBrokers
      .take(missingBrokers)
      .map(_._1)
  }

  def brokersWithPartition(partition: Int, partitionsByBroker: Map[Int, List[Int]]) = partitionsByBroker.filter {
    case (_, partitions) => partitions.contains(partition)
  }.map {
    case (broker, _) => broker
  }.toList


  def newBrokersLoad(actual: Map[BrokerId, Int], affectedBrokers: List[BrokerId], operation: Int => Int): Map[BrokerId, Int] = {
    actual.map {
      case (brokerId, load) if affectedBrokers.contains(brokerId) => (brokerId, operation(load))
      case entry => entry
    }
  }

  def selectPartitionsToReassign(partitions: List[TopicPartition])(implicit config: Config): List[Int] = {
    if (config.partitionsToReassign.isEmpty) {
      println(s"All ${partitions.size} partitions are going to be reassigned")
      partitions.map(_.numOfPartition)
    } else {
      println(s"Partitions to be reassigned: ${config.partitionsToReassign.mkString(",")}")
      config.partitionsToReassign
    }
  }

  //TODO: if config.brokerIds is empty consider as whitelisted brokers all the brokers of the cluster or all the brokers of the topic
  def selectBrokers(partitionsByBroker: Map[Int, List[Int]])(implicit config: Config): List[Int] = {
    def subtract(minuend: List[Int], subtrahend: List[Int]) = minuend.filterNot(subtrahend.contains(_)).toList.sorted

    val actualBrokers = partitionsByBroker.keys.toList
    val whitelistedBrokers = config.brokerIds
    val removedBrokers = subtract(actualBrokers, whitelistedBrokers)
    val newBrokers = subtract(whitelistedBrokers, actualBrokers)
    if (!removedBrokers.isEmpty) {
      println(s"Brokers ${removedBrokers.mkString(",")} will no longer be responsible of ${config.topic}")
    }
    if (!newBrokers.isEmpty) {
      println(s"New brokers for ${config.topic}: ${newBrokers.mkString(",")}")
    }
    println(s"Valid Brokers: ${whitelistedBrokers.mkString(",")}")
    whitelistedBrokers
  }

  protected def extractPropertiesFromLine(line: String): Map[String, String] = {
    line.split("\t").filterNot(_.isEmpty).map(conf => {
      val keyValue = conf.split(":")
      val key = keyValue(0).trim
      if (keyValue.size > 1) (key, keyValue(1).trim) else (key, "")
    }).toMap
  }

  private def finish = {
    println
    println("bye!")
    System.exit(0)
  }

  case class Reassignment(brokersLoad: Map[BrokerId, Int], partitionConf: PartitionConfiguration)

}

