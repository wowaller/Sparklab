package com.cloudera.gmcc.test.ml

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Impurity}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bwo on 2014/12/22.
 */
object DTCase {

  val algo: Algo = org.apache.spark.mllib.tree.configuration.Algo.Classification
  // 5
  //    val impurity: Impurity = Class.forName(props.getProperty(DTCaseContext.IMPURITY, DTCaseContext.DEFAULT_IMPURITY)).asInstanceOf[Impurity]
  val impurity: Impurity = Gini

  def readData(sc: SparkContext, dataPath: String, delimiter: String): RDD[LabeledPoint] = {
    val data = sc.textFile(dataPath)
    data.map { line =>
      val parts = line.toString().split(delimiter)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => {
        if (x.isEmpty) {
          0
        } else {
          x.toDouble
        }
      }).toArray))
    }.cache()
  }

  def randSplit(parsedData: RDD[LabeledPoint], traningRate: Double) = {
    val splits = parsedData.randomSplit(Array(traningRate, 1 - traningRate), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()
    (training, test)
  }

  def trainDT(parsedData: RDD[LabeledPoint], maxDepth: Int) = {
    DecisionTree.train(parsedData, algo, impurity, maxDepth)
  }

  def trainDTRegression(parsedData: RDD[LabeledPoint], maxDepth: Int) = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    DecisionTree.trainRegressor(parsedData, categoricalFeaturesInfo, "variance", maxDepth, 100)
  }

  def trainBayes(parsedData: RDD[LabeledPoint]) = {
    NaiveBayes.train(parsedData)
  }

  def predict(parsedData: RDD[LabeledPoint], model: DecisionTreeModel) = {
    parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
  }

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
    val dataDir = props.getProperty(DTCaseContext.INPUT_PATH)
    // "/user/spark/mnist.scale.t"

    val maxDepth = props.getProperty(DTCaseContext.MAX_DEPTH, DTCaseContext.DEFAULT_MAX_DEPTH).toInt
    // 5
    //    val maxBins = props.getProperty(DTCaseContext.MAX_BINS, DTCaseContext.DEFAULT_MAX_BINS).toInt
    // 100 by default
    //  val quantileCS =
    //    val categoricalFI = props.getProperty(DTCaseContext.CATEGORICAL_FI)
    val trainPercent = props.getProperty(DTCaseContext.TRAINING_PRECENT, DTCaseContext.DEFAULT_TRAINING_PERCENT).toDouble
    val delimiter = props.getProperty(DTCaseContext.DELIMITER, DTCaseContext.DEFAULT_DELIMITER)

    // Load and parse the data file
    //    val data = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](dataDir)//textFile(dataDir)
    //    val data = sc.textFile("hdfs://hadoop2:8020/user/hive/warehouse/formattedboss/*")
    val data = sc.textFile(dataDir)
    val parsedData = data.map { line =>
      val parts = line.toString().split(delimiter)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => {
        if (x.isEmpty) {
          0
        } else {
          x.toDouble
        }
      }).toArray))
    }
    //    val parsedData = MLUtils.loadLibSVMFile(sc, dataDir).cache()
    val splits = parsedData.randomSplit(Array(trainPercent, 1 - trainPercent), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    val model = DecisionTree.train(training, algo, impurity, maxDepth)
    println("============ Top Node of the tree:   " + model.topNode)
    println("============ depth of tree: " + model.depth + "========== Number of Nodes: " + model.numNodes)
    println("=============== Tree Details: ===========")
    println(model.toString)

    //new DecistionTreeModel(topNode: Node, algo: Algo)


    //val algo: Algo     algorithm type -- classification or regression
    //def predict(features: RDD[Vector]): RDD[Double]

    //  Predict values for the given data set using the model trained.

    // Evaluate model on training examples and compute training error
    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //  labelAndPreds.foreach(println)

    val trainErr = labelAndPreds.filter(r => {
      Math.abs(r._1 - r._2) >= 0.5
    }).count.toDouble / test.count
    println("Training Error ========================== " + trainErr)

    // Evaluate model on test examples and compute test error
    //    val labelAndPreds = test.map { point =>
    //      val prediction = model.predict(point.features)
    //      (point.label, prediction)
    //    }
    //  labelAndPreds.foreach(println)
    val testErr = labelAndPreds.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / test.count
    println("Testing Error =========================== " + testErr)

    //Here are are loading our HBase Configuration object.  This will have
    //all the information needed to connect to our HBase cluster.
    //There is nothing different here from when you normally interact with HBase.
    //    val conf = HBaseConfiguration.create();
    //    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    //    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    //
    //    val load = new CertainNumberFilterLoad(sc, conf)
    //    val props = new Properties()
    //    props.load(new FileInputStream(args(0)))
    //    val ssc = load.loadToHbase(props)
    //
    //    ssc.start
    //    ssc.awaitTermination
  }
}
