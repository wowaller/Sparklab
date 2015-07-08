package com.cloudera.spark.streaming.v2

import java.util.Properties

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
class Kafka2DynamicHBase(@transient sc: SparkContext,
                         @transient conf: Configuration,
                         @transient props: Properties)
  extends Kafka2HBase(sc, conf, props) {

  override def load2Hadoop(input: DStream[(String, String)]): Unit = {
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
}
