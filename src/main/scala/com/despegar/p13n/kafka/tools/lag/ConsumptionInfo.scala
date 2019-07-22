package com.despegar.p13n.kafka.tools.lag

import scala.util.Try

sealed abstract case class ConsumptionInfo(topic: String, partition: Int, currentOffset: Long, longEndOffset: Long, lag: Long, consumerId: String, clientId: String)

object ConsumptionInfo {
  def parse[T]: (String => T) => String => Option[T] = func => str => Try(func(str)).toOption

  def fromList(list: List[String]): Option[ConsumptionInfo] = {

    Option(list).filter(_.size == 7).flatMap { l =>
      for {
        partition <- parse(_.toInt)(l(1))
        currentOffset <- parse(_.toLong)(l(2))
        longEndOffset <- parse(_.toLong)(l(3))
        lag <- parse(_.toInt)(l(4))
        val topic = l(0)
        val consumerId = l(5)
        val clientId = l(6)
      } yield new ConsumptionInfo(topic, partition, currentOffset, longEndOffset, lag, consumerId, clientId) {}
    }
  }
}