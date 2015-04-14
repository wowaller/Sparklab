package com.cloudera.gmcc.test.ml

import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.{Gini, Impurity}

/**
 * Created by bwo on 2014/12/22.
 */
object DTCaseContext {
  val INPUT_PATH = "inputPath"
  val ALGORITHM = "algorithm"
  val DEFAULT_ALGORITHM = "org.apache.spark.mllib.tree.configuration.Algo.Classification"
  val IMPURITY = "impurity"
  val DEFAULT_IMPURITY = "org.apache.spark.mllib.tree.impurity.Gini"
  val MAX_DEPTH = "maxDepth"
  val DEFAULT_MAX_DEPTH = "5"
  val MAX_BINS = "maxBins"
  val DEFAULT_MAX_BINS = "100"
  val CATEGORICAL_FI = "categoricalFI"
  val DELIMITER = "delimiter"
  val DEFAULT_DELIMITER = " "
  val TRAINING_PRECENT = "trainingPercent"
  val DEFAULT_TRAINING_PERCENT = "0.8"

//  // load parameters
//  val dataDir = args(0)
//  // "/user/spark/mnist.scale.t"
//  val algo: Algo = Classification
//  // 5
//  val impurity: Impurity = Gini
//  //
//  val maxDepth = args(3).toInt
//  // 5
//  val maxBins = args(4).toInt
//  // 100 by default
//  //  val quantileCS =
//  val categoricalFI = args(6)

}
