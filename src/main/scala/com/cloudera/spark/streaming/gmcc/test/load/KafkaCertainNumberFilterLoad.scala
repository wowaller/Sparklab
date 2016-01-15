package com.cloudera.spark.streaming.gmcc.test.load

import java.io.Serializable
import java.util.Properties

import com.cloudera.gmcc.test.io.{FileRowTextInputFormat, FileRowWritable}
import com.cloudera.gmcc.test.parse.BOSSRecord
import com.cloudera.spark.hbase.HBaseContext
import kafka.serializer.StringDecoder
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
 * Created by bwo on 2014/12/15.
 */
class KafkaCertainNumberFilterLoad(@transient sc: SparkContext,
                                @transient conf: Configuration,
                                @transient props: Properties) extends Serializable {
  val LOG = LogFactory.getLog(classOf[KafkaCertainNumberFilterLoad])

  val USER_MOB_NBR = 4
  val CALL_DT = 12
  val CALL_TIM = 13
  val B_MOB_NBR = 24

  val COLUMN_FAMILY = "info".getBytes
  val QUALIFITER = "common".getBytes()

  val INTERVAL = "sparkstreaming.interval"
  val CHECK_POINT = "sparkstreaming.checkPoint"
  val DEFAULT_INTERVAL = "5000"
  val ENCODING = "sparkstreaming.parse.encoding"
  val DEFAULT_ENCODING = "utf8"
  val TABLE_NAME = "sparkstreaming.hbase.tableName"
  val DELIMTIER = "sparkstreaming.delimiter"
  val DEFAULT_DELIMITER = "&"

  //Kafka parameter
  val KAFKA_TOPICS = "sparkstreaming.kafka.topics"
  val KAFKA_BROKER = "bootstrap.servers"
  val KAFKA_GROUP_ID = "group.id"
  val KAFKA_OFFSET_RESET = "auto.offset.reset"

  @transient var interval: Long = props.getProperty(INTERVAL, DEFAULT_INTERVAL).toLong
  @transient var ssc: StreamingContext = new StreamingContext(sc, Milliseconds(interval))

  ssc.checkpoint(props.getProperty(CHECK_POINT))

  @transient var hbaseContext = new HBaseContext(sc, conf)
  @transient var topics: Set[String] = props.getProperty(KAFKA_TOPICS).split(",").toSet

  var encoding: String = props.getProperty(ENCODING, DEFAULT_ENCODING)
  var tableName: String = props.getProperty(TABLE_NAME)
  var delimiter: String = props.getProperty(DELIMTIER, DEFAULT_DELIMITER)
  var filterNoSet: Set[String] = Set()
  props.getProperty(PropertyContext.FILTER_NUMBER).split(",").foreach(t => {
    filterNoSet += t.trim
  })

  def loadToHbase(props: Properties): StreamingContext = {
    init(props)
    load(deploy())
    //    if(deleteOnComplete) {
    //      files.foreach(f=)
    //    }
    ssc
  }

  def init(props: Properties): StreamingContext = {
    props.getProperty(PropertyContext.FILTER_NUMBER).split(",").foreach(t => {
      filterNoSet += t.trim
    })
    encoding = props.getProperty(PropertyContext.ENCODING, PropertyContext.DEFAULT_ENCODING)
    interval = props.getProperty(PropertyContext.INTERVAL, PropertyContext.DEFAULT_INTERVAL).toInt
    tableName = props.getProperty(PropertyContext.TABLE_NAME)
    ssc = new StreamingContext(sc, Seconds(interval))
    hbaseContext = new HBaseContext(sc, conf)
    ssc
  }

  def deploy(): DStream[(String, String)] = {
    //    LOG.info("Get inputPath as: " + inputPath)
    //Simple example of how you set up a receiver from a HDFS folder
    //    LOG.info("Get inputPath as: " + inputPath)
    //Simple example of how you set up a receiver from a HDFS folder
    var kafkaParameters = Map[String, String]()
    import scala.collection.JavaConversions._
    kafkaParameters = props.toMap
    //    kafkaParameters += (KAFKA_BROKER -> props.getProperty(KAFKA_BROKER))
    //    kafkaParameters += (KAFKA_GROUP_ID -> props.getProperty(KAFKA_GROUP_ID))
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters, topics)

    kafkaStream
  }

  def load(input: DStream[(String, String)]): Unit = {
    input.map {
      case (key, value) => {
        var splits = value.split(delimiter)
        (splits(B_MOB_NBR), (splits(USER_MOB_NBR) + delimiter + splits(CALL_DT) + splits(CALL_TIM) + delimiter + key), value)
      }
    }.filter(t => filterNoSet.contains(t._1)).map{case(bNbr, key, value) => (key, value)}

    hbaseContext.streamBulkPut[(String, String)](
      input, //The input RDD
      tableName, //The name of the table we want to put too
      (t) => {
        //Here we are converting our input record into a put
        //The rowKey is C for Count and a backward counting time so the newest
        //count show up first in HBase's sorted order
        //        LOG.info("Load line " + t._2.toString + " into table " + tableName)


        //We are iterating through the HashMap to make all the columns with their counts
        var put = new Put(t._1.getBytes())
        put.addColumn(COLUMN_FAMILY, QUALIFITER, t._2.getBytes())
      },
      false)
  }
}
