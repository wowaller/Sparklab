package com.cloudera.spark.streaming.v2

import java.util.Properties

import com.cloudera.spark.hbase.HBaseContext
import com.cloudera.spark.streaming.ParseException
import com.cloudera.spark.streaming.parser.GeneralHBaseJsonParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 7/2/15.
 */
class Kafka2HBase(@transient sc: SparkContext,
                  @transient conf: Configuration,
                  @transient props: Properties) extends Kafka2AbstractHadoop(sc, conf, props) {

  val TABLE_NAME = "sparkstreaming.hbase.tableName"
  var tableName: String = props.getProperty(TABLE_NAME)

  @transient var hbaseContext = new HBaseContext(sc, conf)

  override def load2Hadoop(input: DStream[(String, String)]): Unit = {

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
}
