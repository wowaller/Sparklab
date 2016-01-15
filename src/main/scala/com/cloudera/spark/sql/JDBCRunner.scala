package com.cloudera.spark.sql

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 7/18/15.
 */
object JDBCRunner {


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

    var hive = new HiveContext(sc)


    val jdbcDF = hive.load("jdbc", Map(
      "url" -> "jdbc:MySQL://172.31.9.31:3306/scm?user=root&password=cloudera",
      "dbtable" -> "USERS"))

    jdbcDF.registerTempTable("user")

    val ret = hive.sql("select * from user a join uname b on a.USER_NAME = b.username").collect
    ret.foreach(println)
  }
}
