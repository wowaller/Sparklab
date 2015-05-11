package com.cloudera.gmcc.test.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Impurity}
import org.apache.spark.{SparkConf, SparkContext}


object DTCla {
  def main(args: Array[String]) {
    //input: String, algo: Algo, impurity: Impurity, maxDepth: Int, maxBins:Int) {

    //    if (args.length != 7) {
    //      println("Parameters: (input: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]; " +
    //        "algo: org.apache.spark.mllib.tree.configuration.Algo.Algo; " +
    //        "impurity: org.apache.spark.mllib.tree.impurity.Impurity; " +
    //        "maxDepth: Int; maxBins: Int; quantileCalculationStrategy: org.apache.spark" +
    //        ".mllib.tree.configuration.QuantileStrategy.Q
    //      //val categoricalFeaturesInfo = Map[Int, Int]()uantileStrategy; " +
    //      "categoricalFeaturesInfo: Map[Int,Int])")
    //      sys.exit(1)
    //    }

    //input: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint];
    // algo: org.apache.spark.mllib.tree.configuration.Algo.Algo;
    //impurity: org.apache.spark.mllib.tree.impurity.Impurity;
    //maxDepth: Int; maxBins: Int;
    //quantileCalculationStrategy: org.apache.spark.mllib.tree.configuration.QuantileStrategy.QuantileStrategy;
    //categoricalFeaturesInfo: Map[Int,Int]

    //(input: org.apache.spark.rdd.RDD, algo: org.apache.spark.mllib.tree.configuration.Algo.Algo,
    //impurity: org.apache.spark.mllib.tree.impurity.Impurity, maxDepth: Int)
    //(input: org.apache.spark.rdd.RDD, strategy: org.apache.spark.mllib.tree.configuration.Strategy)


    // set up environment
    val conf = new SparkConf()
      .setAppName("Kmeans")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    // load parameters
    val dataDir = args(0)
    // "/user/spark/mnist.scale.t"
    val algo: Algo = org.apache.spark.mllib.tree.configuration.Algo.Classification
    // 5
    val impurity: Impurity = Gini
    //
    val maxDepth = args(3).toInt
    // 5
    val maxBins = args(4).toInt
    // 100 by default
    //  val quantileCS =
    val categoricalFI = args(6)

    // Load and parse the data file
    val data = sc.textFile("hdfs://gzp13:8020/user/hive/warehouse/formattedboss/*")
    val parsedData = data.map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble).toArray))
    }
    //    val parsedData = MLUtils.loadLibSVMFile(sc, dataDir).cache()
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    val model = DecisionTree.train(training, Classification, Gini, maxDepth)
    println("============ Top Node of the tree:   " + model.topNode)
    println("============ depth of tree: " + model.depth + "========== Number of Nodes: " + model.numNodes)
    println("=============== Tree Details: ===========")
    println(model.toString)

    //new DecistionTreeModel(topNode: Node, algo: Algo)


    //val algo: Algo     algorithm type -- classification or regression
    //def predict(features: RDD[Vector]): RDD[Double]

    //  Predict values for the given data set using the model trained.

    // Evaluate model on training examples and compute training error
    val labelAndPreds = training.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //  labelAndPreds.foreach(println)
    val trainErr = labelAndPreds.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / training.count
    println("Training Error ========================== " + trainErr)

    // Evaluate model on test examples and compute test error
    //  val labelAndPreds = test.map { point =>
    //    val prediction = model.predict(point.features)
    //    (point.label, prediction)
    //  }
    //  //  labelAndPreds.foreach(println)
    //  val testErr = labelAndPreds.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / test.count
    //  println("Testing Error =========================== " + testErr)

  }

}

