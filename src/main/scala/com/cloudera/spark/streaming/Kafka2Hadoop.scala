package com.cloudera.spark.streaming

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cloudera.spark.hbase.HBaseContext
import com.cloudera.spark.streaming.parser.{HotelParserJson2Parquet, GeneralHBaseJsonParser}
import kafka.serializer.StringDecoder
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/10/15.
 */
class Kafka2Hadoop(@transient sc: SparkContext,
                  @transient conf: Configuration,
                  @transient props: Properties) extends Serializable {
  val LOG = LogFactory.getLog(classOf[Kafka2Hadoop])

  val INTERVAL = "sparkstreaming.interval"
  val CHECK_POINT = "sparkstreaming.checkPoint"
  val DEFAULT_INTERVAL = "5000";
  val ENCODING = "sparkstreaming.parse.encoding"
  val DEFAULT_ENCODING = "utf8"
  val TABLE_NAME = "sparkstreaming.hbase.tableName"

  //Kafka parameter
  val KAFKA_TOPICS = "sparkstreaming.kafka.topics"
  val KAFKA_BROKER = "bootstrap.servers"
  val KAFKA_GROUP_ID = "group.id"
  val KAFKA_OFFSET_RESET = "auto.offset.reset"

  // HDFS parameter
  val HDFS_PATH_PREFIX = "sparkstreaming.hdfs.outputPathPrefix"
  val HDFS_BATCH_TIME_MS = "sparkstreaming.hdfs.batchTimeMs"
  val DEFAULT_HDFS_BATCH_TIME_MS = "3600000"
  val HDFS_BATCH_PARTITION = "sparkstreaming.hdfs.partition"
  val DEFAULT_HDFS_BATCH_PARTITION = "1"
  val APPEND_DATE_FORMAT = "sparkstreaming.hdfs.appendDate"

  val OUTPUT_TABLE = "sparkstreaming.hive.outputTable"

  @transient var interval: Long = props.getProperty(INTERVAL, DEFAULT_INTERVAL).toLong
  @transient var ssc: StreamingContext = new StreamingContext(sc, Milliseconds(interval))

  ssc.checkpoint(props.getProperty(CHECK_POINT))

  @transient var hbaseContext = new HBaseContext(sc, conf)
  @transient var topics: Set[String] = props.getProperty(KAFKA_TOPICS).split(",").toSet

  var encoding: String = props.getProperty(ENCODING, DEFAULT_ENCODING)
  var tableName: String = props.getProperty(TABLE_NAME)
  var hdfsOutputPath: String = props.getProperty(HDFS_PATH_PREFIX)
  var hdfsBatchTime: Long = props.getProperty(HDFS_BATCH_TIME_MS, DEFAULT_HDFS_BATCH_TIME_MS).toLong
  var hdfsBatchPartition: Int = props.getProperty(HDFS_BATCH_PARTITION, DEFAULT_HDFS_BATCH_PARTITION).toInt
  var appendDateFormat: String = props.getProperty(APPEND_DATE_FORMAT)

  var outputTable: String = props.getProperty(OUTPUT_TABLE)

//  @transient val sqlContext = new SQLContext(sc)
  @transient val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._
//  @transient val parquetFile = sqlContext.parquetFile(hdfsOutputPath)
//  parquetFile.registerTempTable(OUTPUT_TABLE)

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

  def load2HBase(input: DStream[(String, String)]): Unit = {
    var puts = input.map {
      case (key, value) => try {
        GeneralHBaseJsonParser.parse(value)
      } catch {
        case ex: ParseException => {
          LOG.warn("Parse Error.", ex)
          Map[String, ArrayBuffer[Put]]()
        }
      }
    }.flatMap {
      _.values.flatMap(_.toList)
    }

    hbaseContext.streamBulkPut[Put](
      puts, //The input RDD
      tableName, //The name of the table we want to put too
      t => {
        t
      },
      false)
  }

  //    def load(input: DStream[(String, String)]): Unit = {
  //      var puts = input.map{
  //        case (key, value) => try {
  //          HotelParserJson.parse(value)
  //        } catch {
  //          case ex : ParseException => {
  //            LOG.warn("Parse Error.", ex)
  //            null
  //          }
  //        }
  //      }.flatMap{
  //        _.values.toList
  //      }.map(_.toList)
  //
  //      hbaseContext.streamBulkPuts[List[Put]](
  //        puts, //The input RDD
  //        tableName, //The name of the table we want to put too
  //        t => {
  //          t
  //        },
  //        false)
  //    }

  def loadToHBaseTables(input: DStream[(String, String)]): Unit = {
    var puts = input.map {
      case (key, value) => try {
        GeneralHBaseJsonParser.parse(value)
      } catch {
        case ex: ParseException => {
          LOG.error("Parse Error.", ex)
          Map[String, ArrayBuffer[Put]]()
        }
      }
    }.flatMap(_.toList).mapValues(_.toList)

    hbaseContext.streamBulkPutsToTables[(String, List[Put])](
      puts, //The input RDD
      t => t._1, //The name of the table we want to put too
      t => t._2,
      false)
  }

  def loadToHDFSTables(input: DStream[(String, String)]): Unit = {
    var records = input.map {
      case (key, value) => try {
        HotelParserJson2Parquet.parse(value)
      } catch {
        case ex: ParseException => {
          LOG.error("Parse Error.", ex)
          Map[String, ArrayBuffer[HotelParserJson2Parquet.HotelTrans]]()
        }
      }
    }.flatMap {
      _.values.flatMap(_.toList)
    }

    records.window(Milliseconds(hdfsBatchTime), Milliseconds(hdfsBatchTime)).repartition(hdfsBatchPartition).foreachRDD(rdd => {
      import hiveContext.implicits._
//      rdd.toDF().insertInto(outputTable)
      if (appendDateFormat != null) {
        val today = Calendar.getInstance().getTime()
        // create the date/time formatters
        val dateFormat = new SimpleDateFormat(appendDateFormat)
        rdd.toDF().save(hdfsOutputPath + dateFormat.format(today), "parquet", SaveMode.Append)
      }
      else {
        rdd.toDF().save(hdfsOutputPath, "parquet", SaveMode.Append)
      }
//      rdd.toDF().save(hdfsOutputPath, "parquet")
    })
  }
}
