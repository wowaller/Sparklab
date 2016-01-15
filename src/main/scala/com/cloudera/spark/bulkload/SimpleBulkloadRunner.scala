package com.cloudera.spark.bulkload

import com.cloudera.spark.graph.kshell.{KCoreRunner, CsvPasrser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 10/16/15.
 */
object SimpleBulkloadRunner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("Graphx Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    val parser = new SimpleParser()

//    BulkLoadUtil.getRegionStartKeys(new HTable(conf, args(1))).foreach(u => println(new String(u.get())))

    // Create an RDD for the vertices
    BulkLoadUtil.doBulkload[LongWritable, Text, TextInputFormat](sc, conf, args(0), parser, args(1), args(2))

  }
}
