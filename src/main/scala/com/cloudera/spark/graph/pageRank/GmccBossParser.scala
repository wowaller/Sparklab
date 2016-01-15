package com.cloudera.spark.graph.pageRank

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext

/**
 * Created by root on 4/22/15.
 */
object GmccBossParser {

  def parse (sc: SparkContext, input: String) = {
    sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](input).map {
      case (key, value) => {
        var caller = new String(value.getBytes.slice(21, 21 + 24), "gbk")
        var callee = new String(value.getBytes.slice(140, 140 + 24), "gbk")
        (caller, callee)
      }
    }
  }

}
