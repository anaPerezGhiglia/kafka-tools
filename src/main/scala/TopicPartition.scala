import KafkaTools.{BrokerId, maxAmountPerBroker}

case class TopicPartition(numOfPartition: Int, leader: Int, replicas: List[Int], isr: List[Int])

object TopicPartition {

  private val PartitionKey = "Partition"
  private val LeaderKey = "Leader"
  private val ReplicasKey = "Replicas"
  private val IsrKey = "Isr"

  def buildFrom(properties: Map[String, String]): TopicPartition = {
    def buildReplicasList(replicas: String) = replicas.split(",").map(_.toInt).toList

    TopicPartition(properties(PartitionKey).toInt, properties(LeaderKey).toInt, buildReplicasList(properties(ReplicasKey)), buildReplicasList(properties(IsrKey)))
  }
}

case class PartitionConfiguration(partitionNum: Int, replicas: List[Int])


case class DistributionConf(maxBrokerLoad: Int, maxLeadership: Int)

object DistributionConf{
  def calculateFrom(brokers: List[BrokerId], replicationFactor: Int, partitions: List[Int]) = {
    new DistributionConf(maxBrokerLoad(brokers, replicationFactor, partitions), maxBrokerLeadership(brokers, partitions))
  }
  private def maxBrokerLoad(brokers: List[BrokerId], replicationFactor: Int, partitions: List[Int]) = {
    val amountOfReplicas = partitions.size * replicationFactor
    maxAmountPerBroker(amountOfReplicas, brokers)
  }

  private def maxBrokerLeadership(brokers: List[BrokerId], partitions: List[Int]) = {
    maxAmountPerBroker(partitions.size, brokers)
  }
}