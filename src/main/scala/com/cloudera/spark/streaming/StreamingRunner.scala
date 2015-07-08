package com.cloudera.spark.streaming

import java.io.FileInputStream
import java.util.Properties
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 6/11/15.
 */
object StreamingRunner {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: <properties file>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().
      setAppName("Dalian Spark Stream Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)


    //Here are are loading our HBase Configuration object.  This will have
    //all the information needed to connect to our HBase cluster.
    //There is nothing different here from when you normally interact with HBase.
    val conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    val props = new Properties()
    props.load(new FileInputStream(args(0)))
    val load = new Kafka2Hadoop(sc, conf, props)
    val ssc = load.getSSC
    val stream = load.createKafkaInput()
    load.loadToHBaseTables(stream)

    ssc.start
    ssc.awaitTermination
  }
}
