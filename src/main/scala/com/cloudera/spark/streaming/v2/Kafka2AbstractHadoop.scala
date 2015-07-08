package com.cloudera.spark.streaming.v2

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cloudera.spark.hbase.HBaseContext
import com.cloudera.spark.streaming.ParseException
import com.cloudera.streaming.parser.GeneralHBaseJsonParser
import kafka.serializer.StringDecoder
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/10/15.
 */
abstract class Kafka2AbstractHadoop(@transient sc: SparkContext,
                  @transient conf: Configuration,
                  @transient props: Properties) extends Serializable {
  val LOG = LogFactory.getLog(classOf[Kafka2AbstractHadoop])

  val INTERVAL = "sparkstreaming.interval"
  val CHECK_POINT = "sparkstreaming.checkPoint"
  val DEFAULT_INTERVAL = "5000";
  val ENCODING = "sparkstreaming.parse.encoding"
  val DEFAULT_ENCODING = "utf8"

  //Kafka parameter
  val KAFKA_TOPICS = "sparkstreaming.kafka.topics"
  val KAFKA_BROKER = "bootstrap.servers"
  val KAFKA_GROUP_ID = "group.id"
  val KAFKA_OFFSET_RESET = "auto.offset.reset"

  @transient var interval: Long = props.getProperty(INTERVAL, DEFAULT_INTERVAL).toLong
  @transient var ssc: StreamingContext = new StreamingContext(sc, Milliseconds(interval))

  ssc.checkpoint(props.getProperty(CHECK_POINT))

  @transient var topics: Set[String] = props.getProperty(KAFKA_TOPICS).split(",").toSet

  var encoding: String = props.getProperty(ENCODING, DEFAULT_ENCODING)


  def getSSC: StreamingContext = {
    ssc
  }

  def createKafkaRDD() = {
    var kafkaParameters = Map[String, String]()
    import scala.collection.JavaConversions._
    kafkaParameters = props.toMap
    //    kafkaParameters += (KAFKA_BROKER -> props.getProperty(KAFKA_BROKER))
    //    kafkaParameters += (KAFKA_GROUP_ID -> props.getProperty(KAFKA_GROUP_ID))
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters, topics)
  }

  def createKafkaInput(): DStream[(String, String)] = {
    //    LOG.info("Get inputPath as: " + inputPath)
    //Simple example of how you set up a receiver from a HDFS folder
    var kafkaParameters = Map[String, String]()
    import scala.collection.JavaConversions._
    kafkaParameters = props.toMap
    //    kafkaParameters += (KAFKA_BROKER -> props.getProperty(KAFKA_BROKER))
    //    kafkaParameters += (KAFKA_GROUP_ID -> props.getProperty(KAFKA_GROUP_ID))
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters, topics)

    //.createStream(streamingContext,
    //    ssc.fileStream[FileRowWritable, Text, FileRowTextInputFormat](inputPath, (t: Path) => {
    //      if (t.getName.endsWith("_COPYING_")) {
    //        false
    //      } else {
    //        //        if (deleteOnComplete) {
    //        //          files += t
    //        //        }
    //        true
    //      }
    //    }, true).filter(line => filter(line._2.copyBytes()))
    kafkaStream
  }

  def load2Hadoop(input: DStream[(String, String)]);
}
