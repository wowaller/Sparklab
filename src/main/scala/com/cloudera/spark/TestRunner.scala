package com.cloudera.spark

import org.apache.spark._
import org.apache.spark.sql.SQLContext
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

    val sqlContext = new SQLContext(sc)

    val paths = Seq[String]("/user/hive/warehouse/charge_parquet/day=20151025/e74d6a04f91e2bd2-15caeb92f8741dab_1268768961_data.0.parq")
    val cols = Array(
      "timestampstr",
      "eventtype",
      "account_delta",
      "account_owner",
      "rounded_duration",
      "charge_including_free_allowance"
    )
    val df= sqlContext.parquetFile(paths:_*)
    import org.apache.spark.sql.{DataFrame, Row, SQLContext}
    import scala.collection.mutable.ArrayBuffer
    import scala.util.Try
    def rowToArray(row: Row): Array[String] = {
      val ret = new Array[String](row.length)
      for (i <- 0 until row.length) {
        val rowValue = Try(row.get(i)).toOption.getOrElse(new String())
        if (rowValue == null) {
          ret(i) = new String
        }
        else if (rowValue.isInstanceOf[Array[Byte]]) {
          ret(i) = new String(rowValue.asInstanceOf[Array[Byte]])
        } else {
          ret(i) = rowValue.toString
        }
      }
      ret
    }
    val values = df.select(cols.map(c => df.col(c)):_*).rdd.map(u=>rowToArray(u)).collect

  }
}
