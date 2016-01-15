package com.cloudera.spark.streaming.gmcc

import java.io.FileInputStream
import java.util.Properties

import com.cloudera.gmcc.test.io.{FileRowTextInputFormat, FileRowWritable}
import com.cloudera.spark.streaming.gmcc.ps.PsAggregation
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.Text
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 7/22/15.
 */
object SimpleSampleStreaming {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().
      setAppName("GMCC Spark Stream Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(30))
    val input = ssc.fileStream[FileRowWritable, Text, FileRowTextInputFormat]("inputPath", (t: Path) => {
      if (t.getName.endsWith("_COPYING_")) {
        false
      } else {
        true
      }
    }, true).repartition(45).mapPartitions(t => {
      var i : Int = 0
      while (i < 60000) {
        Thread.sleep(10)
        val o = "i"
        i += 1
      }
      t})


    ssc.start
    ssc.awaitTermination
  }

}
