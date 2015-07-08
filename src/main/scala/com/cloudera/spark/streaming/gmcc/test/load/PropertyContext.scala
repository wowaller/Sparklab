package com.cloudera.spark.streaming.gmcc.test.load

/**
 * Created by bwo on 2014/12/15.
 */
object PropertyContext {
  val INPUT_PATH = "inputPath"
  val INTERVAL = "interval"
  val DEFAULT_INTERVAL = "1000"
  val FILTER_NUMBER = "filterNumber"
  val TABLE_NAME = "tableName"
  val ENCODING = "encoding"
  val DEFAULT_ENCODING = "GBK"
  val DELETE_ON_COMPLETE = "deleteOnComplete"
  val DEFAULT_DELETE_ON_COMPLETE = "false"
  val CHECK_POINT = "checkPoint"
  val DEFAULT_CHECK_POINT = "hdfs:///tmp"
}
