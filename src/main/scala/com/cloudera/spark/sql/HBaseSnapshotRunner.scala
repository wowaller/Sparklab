package com.cloudera.spark.sql

import java.io.FileInputStream
import java.util.Properties

import com.cloudera.spark.streaming.parser.GeneralJson2Parquet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 7/19/15.
 */
object HBaseSnapshotRunner {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("testnsnap")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    println("Creating hbase configuration")
    val conf = HBaseConfiguration.create()

    val scan = new Scan()
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    conf.set(TableInputFormat.INPUT_TABLE, "t");

    conf.set(TableInputFormat.SCAN, convertScanToString(new Scan()));
    conf.setStrings("io.serializations", conf.get("io.serializations"),
      classOf[MutationSerialization].getName(),
      classOf[ResultSerialization].getName(),
      classOf[KeyValueSerialization].getName());

    TableSnapshotInputFormatImpl.setInput(conf, "snapshot_t", new Path("hdfs:///tmp"))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    var outputSchema: Array[String] = Array[String]("v")
    val schema = StructType(
      outputSchema.map(fieldName => StructField(fieldName, StringType, true)))

    var rdd = hBaseRDD.flatMapValues(v => {
      v.listCells().toArray
    }).map{case (key, value) => {
      Row.fromSeq(CellUtil.cloneValue(value.asInstanceOf[Cell]).toString.split("&"))
    }}

    var rddDf = hiveContext.createDataFrame(rdd, schema)
    rddDf.registerTempTable("test")

    hBaseRDD.count()

    System.exit(0)
  }

  def convertScanToString(scan : Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }
}
