package com.cloudera.spark.bulkload

import java.util.Properties

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.Logging

/**
  * Created by root on 10/14/15.
  */
class CSVParser extends Parser[(LongWritable, Text), Put] with Serializable with Logging {

  val FAMILY = "spark.hbase.bulkload.csv.family"

  val DELIMITER = "spark.hbase.bulkload.csv.delimiter"
  val DEFAULT_DELIMITER = "\\|"

  val COLUMN = "spark.hbase.bulkload.csv.columns"
  val COLUMN_DELIMITER = "spark.hbase.bulkload.csv.columns.delimiter"
  val DEFAULT_COLUMN_DELIMITER = ","

  val ROWKEY_INDEX = "spark.hbase.bulkload.csv.rowkeyIndex"

  val SALT_NO = "spark.hbase.bulkload.csv.saltNo"
  val DEFAULT_SALT_NO = "0"

  var family: Array[Byte] = new Array[Byte](0)
  var delimiter: String = new String
  var columns: Seq[String] = Seq[String]()
  var rowkeyIndex: Int = 0
  var saltNo: Int = 0

  override def parse(v: (LongWritable, Text)): Put = {
    val line = v._2.toString
    val splits = SplitUtil.split(line, delimiter)
    val rowkey = salt(splits(rowkeyIndex), saltNo) + delimiter + splits(rowkeyIndex)
    val put = new Put(rowkey.getBytes)
    try {
      for (i <- 0 until columns.length) {
        put.addColumn(family, columns(i).getBytes(), splits(i).getBytes())
      }
    } catch {

      case e: ArrayIndexOutOfBoundsException => {
        throw new ParserException("Expected " + columns.length + " fields but got " + splits.length, e)
      };
    }

    put
  }

  override def init(props: Properties): Unit = {
    family = props.getProperty(FAMILY).getBytes()
    delimiter = props.getProperty(DELIMITER, DEFAULT_DELIMITER)
    val columnDelimiter = props.getProperty(COLUMN_DELIMITER, DEFAULT_COLUMN_DELIMITER)
    columns = SplitUtil.split(props.getProperty(COLUMN), columnDelimiter)
    rowkeyIndex = props.getProperty(ROWKEY_INDEX).toInt
    saltNo = props.getProperty(SALT_NO, DEFAULT_SALT_NO).toInt
  }

  /**
    * Get salts for given string by key.hashcode() % count.
    * Length of salts should be the same as given count.
    * @param key The input key
    * @param count
    * @return Salts.
    */
  def salt(key: String, count: Int) : String = {
    if (count <= 1) {
      return ""
    }

    val raw = (Math.abs(key.hashCode() % count)).toString
    val desire = count.toString.length()
    val length = raw.length()
    val sb = new StringBuilder()
    for (i <- length until desire) {
      sb.append(0)
    }
    sb.append(raw)
    return sb.toString()
  }
}
