package kafka.log

import scala.util.matching.Regex

/**
 * Created by aozeritsky on 20.03.2015.
 */
case class BalancerConfig (enable: Boolean, groupMatch: Regex, groupFields: Seq[Int])
