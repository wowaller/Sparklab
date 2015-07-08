package com.cloudera.spark.streaming.gmcc.ps

import java.io.Serializable

/**
 * Created by waller on 3/19/15.
 */
class PSNetRATRecord extends Serializable {

  var firstTime: String = null
  var RAT: String = null
  var totalBytes: Long = 0
  var upBytes: Long = 0
  var downBytes: Long = 0
  var transTime: Double = 0
  var avgUpSpeed: Double = 0
  var avgDownSpeed: Double = 0

  def this(@transient in: Array[String]) = {
    this()
    firstTime = in(UserServiceParseUtil.FIRST_TIME)
    RAT = in(UserServiceParseUtil.RAT)
    upBytes = in(UserServiceParseUtil.UP_BYTES).toLong
    downBytes = in(UserServiceParseUtil.DOWN_BYTES).toLong
    transTime = in(UserServiceParseUtil.TRANS_TIME).toDouble / 100000
  }

  def reduce(other: PSNetRATRecord): PSNetRATRecord = {
    val ret = new PSNetRATRecord(this)
    ret.reduceFrom(other)
    ret
  }

  def this(other: PSNetRATRecord) {
    this()
    if (other == null) {
      firstTime = null
      RAT = null
      totalBytes = 0
      upBytes = 0
      downBytes = 0
      transTime = 0
      avgUpSpeed = 0
      avgDownSpeed = 0

    } else {
      firstTime = other.firstTime
      RAT = other.RAT
      totalBytes = other.totalBytes
      upBytes = other.upBytes
      downBytes = other.downBytes
      transTime = other.transTime
      avgDownSpeed = other.avgDownSpeed
      avgUpSpeed = other.avgUpSpeed
    }

  }

  def reduceFrom(other: PSNetRATRecord): Unit = {
    this.upBytes += other.upBytes
    this.downBytes += other.downBytes
    this.transTime += other.transTime
  }

  def calculate(): Unit = {
    this.avgDownSpeed = downBytes.toDouble / transTime
    this.avgUpSpeed = upBytes.toDouble / transTime
    this.totalBytes = upBytes + downBytes
  }

  override def toString(): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(firstTime).append(",").append(upBytes.toString).append(",").append(downBytes.toString).append(",").append(transTime.toString)
      .append(",").append(avgDownSpeed).append(",").append(avgUpSpeed)
    sb.toString()
  }

}
