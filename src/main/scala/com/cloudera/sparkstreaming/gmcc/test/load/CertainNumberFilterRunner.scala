package com.cloudera.sparkstreaming.gmcc.test.load

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bwo on 2014/12/16.
 */
object CertainNumberFilterRunner {

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

    val load = new CertainNumberFilterLoad(sc, conf)
    val props = new Properties()
    props.load(new FileInputStream(args(0)))
    val ssc = load.loadToHbase(props)

    ssc.start
    ssc.awaitTermination
  }
}
