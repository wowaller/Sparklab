package com.cloudera.spark.streaming.parser

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/10/15.
 */
trait ListParser {

  case class Trans()

  def parse(line: String): Map[String, ArrayBuffer[Trans]]
}
