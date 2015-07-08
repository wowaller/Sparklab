package com.cloudera.spark.streaming.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cloudera.spark.streaming.ParseException
import com.cloudera.spark.streaming.parser.GeneralJson2Parquet
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 7/2/15.
 */
class Kafka2HDFS(@transient sc: SparkContext,
                 @transient conf: Configuration,
                 @transient props: Properties)
  extends Kafka2AbstractHadoop(sc, conf, props) {

  // HDFS parameter
  val HDFS_PATH_PREFIX = "sparkstreaming.hdfs.outputPathPrefix"
  val HDFS_BATCH_TIME_MS = "sparkstreaming.hdfs.batchTimeMs"
  val DEFAULT_HDFS_BATCH_TIME_MS = "3600000"
  val HDFS_BATCH_PARTITION = "sparkstreaming.hdfs.partition"
  val DEFAULT_HDFS_BATCH_PARTITION = "1"
  val APPEND_DATE_FORMAT = "sparkstreaming.hdfs.appendDate"
  val OUTPUT_SCHEMA = "sparkstreaming.hdfs.schema"

  // Hive parameter
  val OUTPUT_TABLE = "sparkstreaming.hive.outputTable"

  @transient val hiveContext = new HiveContext(sc)

  var hdfsOutputPath: String = props.getProperty(HDFS_PATH_PREFIX)
  var hdfsBatchTime: Long = props.getProperty(HDFS_BATCH_TIME_MS, DEFAULT_HDFS_BATCH_TIME_MS).toLong
  var hdfsBatchPartition: Int = props.getProperty(HDFS_BATCH_PARTITION, DEFAULT_HDFS_BATCH_PARTITION).toInt
  var appendDateFormat: String = props.getProperty(APPEND_DATE_FORMAT)
  var outputSchema: Array[String] = GeneralJson2Parquet.parseSchema(props.getProperty(OUTPUT_SCHEMA))
  val schema = StructType(
    outputSchema.map(fieldName => StructField(fieldName, StringType, true)))

  var outputTable: String = props.getProperty(OUTPUT_TABLE)


  override def load2Hadoop(input: DStream[(String, String)]): Unit = {
    var records: DStream[Row] = input.map {
      case (key, value) => try {
        GeneralJson2Parquet.parse(value, outputSchema)
      } catch {
        case ex: ParseException => {
          LOG.error("Parse Error.", ex)
          Map[String, ArrayBuffer[Row]]()
        }
      }
    }.flatMap {
      _.values.flatMap(_.toList)
    }

    records.window(Milliseconds(hdfsBatchTime), Milliseconds(hdfsBatchTime)).repartition(hdfsBatchPartition).foreachRDD(rdd => {
      //      rdd.toDF().insertInto(outputTable)
      if (appendDateFormat != null) {
        val today = Calendar.getInstance().getTime()
        // create the date/time formatters
        val dateFormat = new SimpleDateFormat(appendDateFormat)
        hiveContext.createDataFrame(rdd, schema).save(hdfsOutputPath + dateFormat.format(today), "parquet", SaveMode.Append)
      }
      else {
        hiveContext.createDataFrame(rdd, schema).save(hdfsOutputPath, "parquet", SaveMode.Append)
      }
      //      rdd.toDF().save(hdfsOutputPath, "parquet")
    })
  }
}
