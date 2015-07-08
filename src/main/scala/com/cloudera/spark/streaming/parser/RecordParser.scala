package com.cloudera.spark.streaming.parser

import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/10/15.
 */
trait RecordParser{
  def parse(key: String, line: String, delimiter : String) : Put
}
