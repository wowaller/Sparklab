package com.cloudera.spark.bulkload

import java.util

import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.{KeyValue, CellUtil, KeyValueUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
  * Created by root on 1/5/16.
  */
object Test {
  def main(args: Array[String]): Unit = {
//    val put: Put = new Put("1".getBytes())
//    put.addColumn("f".getBytes(), "c".getBytes(), "1".getBytes())
//    println(put)
//
//    import scala.collection.JavaConversions._
//    val ret: mutable.MutableList[(Array[Byte], Array[Byte])] = mutable.MutableList()
//    for (cells <- put.getFamilyCellMap.values) {
//      for (cell <- cells) {
//        ret.+=((put.getRow, KeyValueUtil.copyToNewByteArray(KeyValueUtil.ensureKeyValue(cell))))
//      }
//    }
//
//    val put2: Put = new Put("1".getBytes())
//    ret.foreach( unit => {
//      val cell = new KeyValue(unit._2)
//      put2.add(cell)
//    })
//    println(put2)

//    var line = "1|2|3"
//    line.split("\\|").foreach(println)

    val num = 200
    val length = 3

    for(i <- 0 until num) {
      var ret = String.valueOf(i)
      var l = length - ret.length()
      for(j <- 0 until l) {
        ret = "0" + ret
      }
      print(ret + ",")
    }

//    println(getPartition("10".getBytes()))

//    val test1 = "test".getBytes()
//    val test2 = "test".getBytes()
//    println(WritableComparator.compareBytes(test1, 0, test1.length,test2, 0, test2.length))
  }

  var splits = List(
    "01".getBytes(),
    "03".getBytes(),
    "09".getBytes(),
    "11".getBytes()
  )
  var numPartitions = splits.size

  def getPartition(key: Array[Byte]): Int = {
    if (compareBytes(key, splits(numPartitions - 1)) >= 0) {
      numPartitions
    } else {
      getPartition(key, 0, numPartitions)
    }
  }

  def getPartition(key: Array[Byte], from: Int, to: Int): Int = {
    if ((to - from) <= 1) {
      return from
    }
    val mid = (from + to) / 2
    val diff = compareBytes(key, splits(mid))
    if (diff > 0) {
      getPartition(key, mid, to)
    } else if (diff < 0) {
      getPartition(key, from, mid)
    } else {
      mid
    }
  }

  def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length)
  }
}
