package com.cloudera.spark.bulkload

import java.util.Properties

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.io.{Text, LongWritable}

/**
 * Created by root on 10/14/15.
 */
class SimpleParser extends Parser[(LongWritable, Text), Put] with Serializable {

  val family = "f".getBytes()
  val column = "c".getBytes()

  override def parse(v: (LongWritable, Text)): Put = {
    val str = v._2.toString
    val put = new Put(str.split(",")(0).getBytes)
    put.addColumn(family, column, str.getBytes())
    put
  }

  override def init(props: Properties): Unit = {

  }
}
