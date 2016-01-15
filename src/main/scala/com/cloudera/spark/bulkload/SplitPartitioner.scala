package com.cloudera.spark.bulkload

import com.cloudera.spark.bulkload.serializable.BytesSerializable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{WritableComparator, BytesWritable}
import org.apache.spark.Partitioner

/**
 * Created by root on 10/15/15.
 */
class SplitPartitioner(splits: List[Array[Byte]]) extends Partitioner {
  override def numPartitions: Int = splits.size

  override def getPartition(key: Any): Int =  {
//    import scala.util.control.Breaks._
    if (key.isInstanceOf[Array[Byte]]) {
      getPartition(key.asInstanceOf[Array[Byte]])
    } else if (key.isInstanceOf[BytesSerializable]) {
      getPartition(key.asInstanceOf[BytesSerializable].getBytes())
    } else if (key.isInstanceOf[String]) {
      getPartition(key.asInstanceOf[String].getBytes())
    } else {
      0
    }
  }

  def getPartition(key: Array[Byte]): Int = {
    if (compareBytes(key, splits(numPartitions - 1)) >= 0) {
      numPartitions - 1
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
