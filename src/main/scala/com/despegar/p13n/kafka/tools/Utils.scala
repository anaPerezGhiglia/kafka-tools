package com.despegar.p13n.kafka.tools

object Utils {

  def bye = {
    println
    println("bye!")
    System.exit(0)
  }


  implicit class EnhancedConfig(config: Config){
    def copyReassign(newConf: ReassignConfig) = config.copy(reassignConfig = newConf)
    def copyLag(newConf: LagConfig) = config.copy(lagConfig = newConf)
  }
}
