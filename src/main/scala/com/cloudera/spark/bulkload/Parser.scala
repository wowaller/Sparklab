package com.cloudera.spark.bulkload

import java.util.Properties

/**
 * Created by root on 10/14/15.
 */
trait Parser[I, O] {
  def init(props: Properties)
  @throws(classOf[ParserException])
  def parse(input: I) : O
}
