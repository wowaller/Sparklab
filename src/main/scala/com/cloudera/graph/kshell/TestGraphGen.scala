package com.cloudera.graph.kshell

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.util.Random

/**
 * Created by root on 4/22/15.
 */
object TestGraphGen extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("Graphx Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)
    // Create an RDD for the vertices

    var v = 1
    var partition = 1
    var k = 1
    var round = 1
    var itrEachRound = 1
    var output = "hdfs:///user/bwo/result"
    if (args.length < 6) {
      printUsage()
      System.exit(0)
    } else if (args.length == 6) {
      output = args(0)
      v = args(1).toInt
      partition = args(2).toInt
      k = args(3).toInt
      round = args(4).toInt
      itrEachRound = args(5).toInt
    }

    //    val cdrData = sc.textFile(args(0)).map(line => {
    //      var splited = line.split("\\|")
    //      (splited(0).trim, splited(2).trim)
    //    })

    logWarning("Generating for each vertices.")
    var parts = new Array[(Int, Int, Int, Int)](v)
    for (i <- 0 until v) {
      parts(i) = (i, Math.abs(Random.nextInt()) % k, v, itrEachRound)
    }

    var gen = sc.parallelize(parts, partition)

    var ret = gen.flatMap(genPairs)
    for (i <- 0 until round) {
      logWarning("Generating for " + i + "th round.")
      ret = ret.union(gen.flatMap(genPairs)).persist.repartition(partition)
    }

    ret.map{case (er, ee) => er+"|"+ee}.saveAsTextFile(output)

  }

  def genPairs(tuple : (Int, Int, Int, Int)) = {
    var id = tuple._1
    var k = tuple._2
    var total = tuple._3
    var itrEachRound = tuple._4

    var ret = new Array[(Int, Int)](k * itrEachRound)

    for (l <- 0 until itrEachRound) {
      for (j <- 0 until k) {
        ret(k * l + j) = (id, Math.abs(Random.nextInt()) % total)
      }
    }
    ret
  }

  def printUsage(): Unit = {
    println("Usage: cmd <output> <vertex> <partition> <k> <round> <itrPerRound>")
  }
}
