package com.despegar.p13n.kafka.tools

import com.despegar.p13n.kafka.tools.lag.Lag
import com.despegar.p13n.kafka.tools.reassign._

object KafkaTools extends App {

  type BrokerId = Int

  val parser = new ReassignPartitionsParser
  parser.parse(args, Config()) match {
    case Some(config) => {
      config.command match {
        case Command.NONE => parser.showUsage
        case Command.GENERATE_PARTITIONS_REASSIGNMENTS => Reassign.generatePartitionsReassignments(config.reassignConfig)
        case Command.CHECK_LAG => Lag.checkLag(config.lagConfig)
      }
    }
    case None => Unit // arguments are bad, error message will have been displayed

  }

}

