package com.cloudera.spark.graph.pageRank

import java.io.FileInputStream
import java.util.Properties

import com.cloudera.gmcc.test.ml.DTCaseContext
import com.cloudera.spark.graph.kshell.CsvPasrser
import org.apache.spark.graphx.{PartitionStrategy, Graph, Edge}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 7/18/15.
 */
object PageRankRunner {
  val INPUT = "spark.pagerank.inputPath"
  val ITERATION = "spark.pagerank.interation"
  val DEFAULT_ITERATION = "1000"
  val RESIDUAL_PROBABILITY = "spark.pagerank.probablity"
  val DEFAULT_RESIDUAL_PROBABILITY = "0.15"


  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Error: input required <properties_file>");
      sys.exit(1)
    }

    val props = new Properties()
    props.load(new FileInputStream(args(0)))

    val sparkConf = new SparkConf().
      setAppName("GMCC Machine Learning Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)

    //input: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint];
    // algo: org.apache.spark.mllib.tree.configuration.Algo.Algo;
    //impurity: org.apache.spark.mllib.tree.impurity.Impurity;
    //maxDepth: Int; maxBins: Int;
    //quantileCalculationStrategy: org.apache.spark.mllib.tree.configuration.QuantileStrategy.QuantileStrategy;
    //categoricalFeaturesInfo: Map[Int,Int]

    //(input: org.apache.spark.rdd.RDD, algo: org.apache.spark.mllib.tree.configuration.Algo.Algo,
    //impurity: org.apache.spark.mllib.tree.impurity.Impurity, maxDepth: Int)
    //(input: org.apache.spark.rdd.RDD, strategy: org.apache.spark.mllib.tree.configuration.Strategy)

    // load parameters
    val dataDir = props.getProperty(INPUT)
    // "/user/spark/mnist.scale.t"

    val iter = props.getProperty(ITERATION, DEFAULT_ITERATION).toInt
    val resProb = props.getProperty(RESIDUAL_PROBABILITY, DEFAULT_RESIDUAL_PROBABILITY).toDouble
    // 5
    //    val maxBins = props.getProperty(DTCaseContext.MAX_BINS, DTCaseContext.DEFAULT_MAX_BINS).toInt
    // 100 by default
    //  val quantileCS =
    //    val categoricalFI = props.getProperty(DTCaseContext.CATEGORICAL_FI)
    val delimiter = props.getProperty(DTCaseContext.DELIMITER, DTCaseContext.DEFAULT_DELIMITER)

    // Load and parse the data file
    //    val data = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](dataDir)//textFile(dataDir)
    //    val data = sc.textFile("hdfs://hadoop2:8020/user/hive/warehouse/formattedboss/*")
    val cdrData = CsvPasrser.parse(sc, dataDir).persist(StorageLevel.MEMORY_AND_DISK)

    val vertex = cdrData.flatMap(line => {
      Seq(line._1, line._2)
    }).distinct().zipWithUniqueId().cache()

    val cdrIdData = cdrData.map{
      case (caller, callee) => ((caller, callee), 1)
    }.reduceByKey((a, b) => a+b).map{
      case ((caller, callee), v) => {
        (caller, (callee, v))
      }
    }.join(vertex).map {
      case (caller, ((callee, v), callerId)) => (callee, (callerId, v))
    }.join(vertex).map {
      case (callee, ((callerId, v), calleeId)) => (callerId, calleeId, v)
    }

    val edges = cdrIdData.map{
      case (callerId, calleeId, v) => {
        Edge(callerId, calleeId, v.toDouble)
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val defaultUser = ("nobady")

    var graph = Graph[String, Double](vertex.map { case (value, id) => (id, value) }, edges, defaultUser)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    WeightedPageRank.run[String](graph, iter, resProb)
  }
}
