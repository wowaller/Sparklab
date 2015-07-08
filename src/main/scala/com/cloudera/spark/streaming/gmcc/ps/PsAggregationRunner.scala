package com.cloudera.spark.streaming.gmcc.ps

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by waller on 3/19/15.
 */
object PsAggregationRunner {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().
      setAppName("GMCC Spark Stream Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)

    //Here are are loading our HBase Configuration object.  This will have
    //all the information needed to connect to our HBase cluster.
    //There is nothing different here from when you normally interact with HBase.
    val conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    val aggregate = new PsAggregation(sc, conf)
    val props = new Properties()
    props.load(new FileInputStream(args(0)))
    val ssc = aggregate.init(props)
    val stream = aggregate.createSource()
    aggregate.aggregateNetLAC(stream)
    aggregate.aggregateBusLAC(stream)

    ssc.start
    ssc.awaitTermination
  }
}
