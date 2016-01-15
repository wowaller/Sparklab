package com.cloudera.spark.bulkload

import java.io.FileReader
import java.util.Properties

import com.cloudera.spark.bulkload.serializable.{BytesSerializer, BytesSerializable}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 10/16/15.
 */
object BulkloadRunner {

  val INPUT_PATH = "spark.hbase.bulkload.input"
  val TABLE = "spark.hbase.bulkload.tablename"
  val HFILE_PATH = "spark.hbase.bulkload.hfilepath"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("Graphx Test").
      set("spark.cleaner.ttl", "120000")

    sparkConf.registerKryoClasses(Array(classOf[BytesSerializable], classOf[BytesSerializer]))

    val props = new Properties()
    props.load(new FileReader(args(0)))

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()

    val parser = new CSVParser()
    parser.init(props)

    val inputPath = props.getProperty(INPUT_PATH)
    val tableName = props.getProperty(TABLE)
    val hfilePath = props.getProperty(HFILE_PATH)

    // Create an RDD for the vertices
    BulkLoadUtil.doBulkload[LongWritable, Text, TextInputFormat](sc, conf, inputPath, parser, tableName, hfilePath)

  }
}
