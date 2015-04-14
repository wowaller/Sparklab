package com.cloudera.sparkstreaming.gmcc.test.load

import java.io.Serializable
import java.util
import java.util.Properties

import com.cloudera.gmcc.test.io.{FileRowTextInputFormat, FileRowWritable}
import com.cloudera.gmcc.test.parse.BOSSRecord
import com.cloudera.spark.hbase.HBaseContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by bwo on 2014/12/15.
 */
class CertainNumberFilterLoad(@transient sc: SparkContext,
                              @transient conf: Configuration) extends Serializable{
  val LOG = LogFactory.getLog(classOf[CertainNumberFilterLoad])
  val PHONE_NUM_LENGTH: Int = 64
  val BPHONE_NUM_LENGTH: Int = 32

  @transient var ssc: StreamingContext = null

  var inputPath: String = null
  var interval: Int = 0
  var filterNoSet: Set[String] = Set()
  var encoding: String = null
  var deleteOnComplete: Boolean = false
  var hbaseContext: HBaseContext = null
  var tableName: String = null

  def init(props: Properties): StreamingContext = {
    inputPath = props.getProperty(PropertyContext.INPUT_PATH)
    props.getProperty(PropertyContext.FILTER_NUMBER).split(",").foreach(t => {
      filterNoSet += t.trim
    })
    encoding = props.getProperty(PropertyContext.ENCODING, PropertyContext.DEFAULT_ENCODING)
    interval = props.getProperty(PropertyContext.INTERVAL, PropertyContext.DEFAULT_INTERVAL).toInt
    tableName = props.getProperty(PropertyContext.TABLE_NAME)
    deleteOnComplete = props.getProperty(PropertyContext.DELETE_ON_COMPLETE, PropertyContext.DEFAULT_DELETE_ON_COMPLETE).toBoolean
    ssc = new StreamingContext(sc, Seconds(interval))
    hbaseContext = new HBaseContext(sc, conf)
    ssc
  }

  def deploy(): DStream[(FileRowWritable, Text)] = {
//    LOG.info("Get inputPath as: " + inputPath)
    //Simple example of how you set up a receiver from a HDFS folder
    ssc.fileStream[FileRowWritable, Text, FileRowTextInputFormat](inputPath, (t: Path) => {
      if (t.getName.endsWith("_COPYING_")) {
        false
      } else {
//        if (deleteOnComplete) {
//          files += t
//        }
        true
      }
    }, true).filter(line => filter(line._2.copyBytes()))
  }

  def filter(input: Array[Byte]): Boolean = {
    val bSubno = new String(input.slice(180, 180 + BPHONE_NUM_LENGTH)).trim
    val subno = new String(input.slice(21, 21 + PHONE_NUM_LENGTH)).trim
    LOG.info("Caller : " + subno + ", Callee : " + bSubno)
    (filterNoSet.contains(subno) || filterNoSet.contains(bSubno))
  }

  def load(input: DStream[(FileRowWritable, Text)]): Unit = {
    hbaseContext.streamBulkPut[(FileRowWritable, Text)](
      input, //The input RDD
      tableName, //The name of the table we want to put too
      (t) => {
        val record = new BOSSRecord()
        record.setStoreEncoding(encoding)
        //Here we are converting our input record into a put
        //The rowKey is C for Count and a backward counting time so the newest
        //count show up first in HBase's sorted order
//        LOG.info("Load line " + t._2.toString + " into table " + tableName)
        record.parse(t._2.copyBytes(), t._1.getFileName, t._1.getRow)

        // create row key
        val rowKey = record.getHBaseRowKey();
        val put = new Put(Bytes.toBytes(rowKey));

        record.getHBaseColumns.foreach(column => {
          val value = record.getHBaseColumnValueInBytes(column.family, column.column);
          if (value != null) {
            put.add(column.family_bytes, column.column_bytes, value);
          }
        })
        //We are iterating through the HashMap to make all the columns with their counts
        put
      },
      false)
  }

  def loadToHbase(props: Properties): StreamingContext = {
    init(props)
    load(deploy())
//    if(deleteOnComplete) {
//      files.foreach(f=)
//    }
    ssc
  }
}
