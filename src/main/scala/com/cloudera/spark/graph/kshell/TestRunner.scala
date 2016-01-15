package com.cloudera.spark.graph.kshell

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.storage.StorageLevel

/**
 * Created by root on 4/9/15.
 */
object TestRunner extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("Graphx Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)
    // Create an RDD for the vertices

    var k = 0
    var input: String = null
    var kmin: Int = 1
    var top = -1
    if (args.length < 2) {
      printUsage()
      System.exit(0)
    } else if (args.length == 2) {
      input = args(0)
      k = args(1).toInt
      top = -1
    } else if (args.length == 4) {
      input = args(0)
      k = args(1).toInt
      kmin = args(2).toInt
      top = args(3).toInt
    }

//    val cdrData = sc.textFile(args(0)).map(line => {
//      var splited = line.split("\\|")
//      (splited(0).trim, splited(2).trim)
//    })

//    val cdrData = GmccBossParser.parse(sc, input)
    val cdrData = CsvPasrser.parse(sc, input).persist(StorageLevel.MEMORY_AND_DISK)

    KCoreRunner.run(sc, cdrData, k, kmin, top)

  }

  def printUsage(): Unit = {
    println("Usage: cmd <input> <k>")
  }
}
