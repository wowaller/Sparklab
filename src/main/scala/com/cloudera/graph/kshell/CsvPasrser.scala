package com.cloudera.graph.kshell

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext

/**
 * Created by root on 4/27/15.
 */
object CsvPasrser {

  def parse (sc: SparkContext, input: String) = {
    sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](input).map {
      case (key, value) => {
        var vSplit = value.toString.split(",")
        (vSplit(0), vSplit(1))
      }
    }
  }

}
