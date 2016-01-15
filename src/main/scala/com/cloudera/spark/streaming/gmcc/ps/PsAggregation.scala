package com.cloudera.spark.streaming.gmcc.ps

import java.io.Serializable
import java.util.Properties

import com.cloudera.spark.hbase.HBaseContext
import com.cloudera.spark.streaming.gmcc.test.load.PropertyContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by waller on 3/19/15.
 */
class PsAggregation(@transient sc: SparkContext,
                    @transient conf: Configuration) extends Serializable {
  val LOG = LogFactory.getLog(classOf[PsAggregation])

  @transient var ssc: StreamingContext = null
  @transient var distinctIMSI: RDD[(String, String)] = null

  var inputPath: String = null
  var interval: Int = 0
  var encoding: String = null
  var hbaseContext: HBaseContext = null
  var tableName: String = null
  var checkPoint: String = null
  //  var netLacRdd : RDD[(String, PSNetLACRecord)] = sc.emptyRDD

  def init(props: Properties): StreamingContext = {
    inputPath = props.getProperty(PropertyContext.INPUT_PATH)
    encoding = props.getProperty(PropertyContext.ENCODING, PropertyContext.DEFAULT_ENCODING)
    interval = props.getProperty(PropertyContext.INTERVAL, PropertyContext.DEFAULT_INTERVAL).toInt
    tableName = props.getProperty(PropertyContext.TABLE_NAME)
    checkPoint = props.getProperty(PropertyContext.CHECK_POINT, PropertyContext.DEFAULT_CHECK_POINT)
    ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint(checkPoint)
    hbaseContext = new HBaseContext(sc, conf)
    distinctIMSI = sc.emptyRDD
    ssc
  }

  def createSource(): DStream[(String, Array[String])] = {
    var source = UserServiceParseUtil.HDFSFileSource(ssc, inputPath).map(line => {
      var key: String = UserServiceParseUtil.clusterTime(line(UserServiceParseUtil.FIRST_TIME))
      (key, line)
    }).persist(StorageLevel.MEMORY_AND_DISK)
    source
  }

  def aggregateNetLAC(source: DStream[(String, Array[String])]): Unit = {
    val result = source.map(line => {
      val ret = new PSNetLACRecord(line._2)
      (ret.getKey(), ret)
    }).reduceByKey((a, b) => {
      val ret = new PSNetLACRecord(a)
      ret.reduceFrom(b)
      ret
    }).updateStateByKey(updateNetLAC).cache()

    result.foreachRDD(rdd => {
      rdd.collect().foreach(unit => {
        println(unit._2.toString())
      })
    })

    //      .transform(rdd => {
    //      rdd.leftOuterJoin(netLacRdd).map(line => {
    //        line._2._1.reduceFrom(line._2._2.get)
    //        (line._1, line._2)
    //      })
    //    }).foreachRDD(rdd => {
    //
    //    })

    hbaseContext.streamBulkPut[(String, PSNetLACRecord)](
      result, //The input RDD
      tableName, //The name of the table we want to put too
      (t) => {
        val put = new Put(("netLAC_" + t._1).getBytes)
        put.add("f".getBytes(), "c".getBytes(), t._2.toString.getBytes())
        //We are iterating through the HashMap to make all the columns with their counts
        put
      },
      false)
  }

  //    def updateNetLAC(newValues: Seq[(String, String, Long, Long, Double)], oldCount: Option[(String, String, Long, Long, Double)]): Option[(String, String, Long, Long, Double)] = {
  //      var ret = oldCount.getOrElse(null)
  //      newValues.foreach(line => {
  //        if (ret == null) {
  //          ret = (line._1, line._2, line._3, line._4, line._5)
  //        }
  //        else {
  //          ret = (ret._1, ret._2, ret._3 + line._3, ret._4 + line._4, ret._5 + line._5)
  //        }
  //      })
  //      Some(ret)
  //    }

  def updateNetLAC(newValues: Seq[PSNetLACRecord], oldCount: Option[PSNetLACRecord]): Option[PSNetLACRecord] = {
    var ret = oldCount.getOrElse(null)
    newValues.foreach(line => {
      if (ret == null) {
        ret = new PSNetLACRecord(line)
      }
      else {
        ret.reduceFrom(line)
      }
    })
    ret.calculate()
    Some(ret)
  }


  //  def aggregateNetLAC(source : DStream[(String, Array[String])]): Unit = {
  //        val result = source.map(line => {
  //          val ret = PSNetLACRecordUtil.extract(line._2)
  //          (line._1 + "_" + ret._2, ret)
  //        }).reduceByKey((a, b) => {
  //          (a._1, a._2, a._3 + b._3, a._4 + b._4, a._5 + b._5)
  //        }).updateStateByKey(updateNetLAC)
  //
  //        result.foreachRDD(rdd => {
  //          rdd.collect().foreach(unit => {
  //            println(unit._2.toString())
  //          })
  //        })
  //
  //
  //        hbaseContext.streamBulkPut[(String, (String, String, Long, Long, Double))](
  //          result, //The input RDD
  //          tableName, //The name of the table we want to put too
  //          (t) => {
  //            val put = new Put(("netLAC_" + t._1).getBytes)
  //            put.add("f".getBytes(), "c".getBytes(), t._2.toString.getBytes())
  //            //We are iterating through the HashMap to make all the columns with their counts
  //            put
  //          },
  //          false)
  //      }

  def aggregateBusLAC(source: DStream[(String, Array[String])]): Unit = {
    // Perse and aggregate for each firsttime+LAC+serviceGroup
    val parsed = source.map(line => {
      val record = new PSBusLACRecord(line._2)
      (record.getKey(), record)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val result = parsed.reduceByKey((a, b) => {
      val ret = new PSBusLACRecord(a)
      ret.reduceFrom(b)
      ret
    }).updateStateByKey(updateBusLAC).map(line => {
      (line._2.firstTime, line._2)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //    val distinct = parsed.mapValues(value => {
    //      value.imsi
    //    }).transform(rdd => {
    //      rdd.union(distinctIMSI)
    //    }).persist(StorageLevel.MEMORY_AND_DISK)

    val distinct = parsed.mapValues(value => {
      value.imsi
    }).updateStateByKey(updateDistinctIMSI).mapValues(value => {
      value.count(a => true)
    })


    // Count total bytes per interval
    val totalBytes = result.mapValues(line => {
      line.totalBytes
    }).reduceByKey((a, b) => {
      a + b
    }).updateStateByKey(sum)


    // Compute percentage for each value
    val result2 = result.join(totalBytes).mapValues(line => {
      line._1.bytePrecent = line._1.totalBytes.toDouble / line._2
      line._1
    }).map(line => (line._2.getKey(), line._2)).join(distinct).mapValues(value => {
      value._1.distinctIMSI = value._2
      value._1
    }).persist(StorageLevel.MEMORY_AND_DISK)

    //    distinct.foreachRDD(rdd => {
    //      distinctIMSI = distinctIMSI.union(rdd)
    //    })

    result2.foreachRDD(rdd => {
      rdd.collect().foreach(unit => {
        println(unit._2.toString())
      })
    })

    hbaseContext.streamBulkPut[(String, PSBusLACRecord)](
      result2, //The input RDD
      tableName, //The name of the table we want to put too
      (t) => {
        val put = new Put(("busLAC_" + t._1).getBytes)
        put.add("f".getBytes(), "c".getBytes(), t._2.toString.getBytes())
        //We are iterating through the HashMap to make all the columns with their counts
        put
      },
      false)
  }

  def updateBusLAC(newValues: Seq[PSBusLACRecord], oldCount: Option[PSBusLACRecord]): Option[PSBusLACRecord] = {
    var ret = oldCount.getOrElse(null)
    newValues.foreach(line => {
      if (ret == null) {
        ret = new PSBusLACRecord(line)
      }
      else {
        ret.reduceFrom(line)
      }
    })
    ret.calculate()
    Some(ret)
  }

  def updateDistinctIMSI(newValues: Seq[String], old: Option[Set[String]]): Option[Set[String]] = {
    var ret: Set[String] = old.getOrElse(Set[String]())
    newValues.foreach(value => {
      ret += value
    })
    Some(ret)
  }

  def sum(newValues: Seq[Long], oldCount: Option[Long]): Option[Long] = {
    var ret: Long = oldCount.getOrElse(0)
    newValues.foreach(line => {
      ret += line
    })
    Some(ret)
  }
}
