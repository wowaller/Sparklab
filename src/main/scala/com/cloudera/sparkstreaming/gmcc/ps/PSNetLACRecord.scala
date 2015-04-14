package com.cloudera.sparkstreaming.gmcc.ps

import java.io.Serializable

/**
 * Created by waller on 3/19/15.
 */
class PSNetLACRecord extends Serializable {

  var firstTime: String = null
  var LAC: String = null
  var totalBytes: Long = 0
  var upBytes: Long = 0
  var downBytes: Long = 0
  var transTime: Double = 0
  var avgUpSpeed: Double = 0
  var avgDownSpeed: Double = 0

  def this(@transient in: Array[String]) = {
    this()
    firstTime = UserServiceParseUtil.clusterTime(in(UserServiceParseUtil.FIRST_TIME))
    LAC = in(UserServiceParseUtil.LAC)
    upBytes = in(UserServiceParseUtil.UP_BYTES).toLong
    downBytes = in(UserServiceParseUtil.DOWN_BYTES).toLong
    transTime = in(UserServiceParseUtil.TRANS_TIME).toDouble / 100000
  }

  def reduce(other: PSNetLACRecord): PSNetLACRecord = {
    val ret = new PSNetLACRecord(this)
    ret.reduceFrom(other)
    ret
  }

  def this(other: PSNetLACRecord) {
    this()
    if (other == null) {
      firstTime = null
      LAC = null
      totalBytes = 0
      upBytes = 0
      downBytes = 0
      transTime = 0
      avgUpSpeed = 0
      avgDownSpeed = 0

    } else {
      firstTime = other.firstTime
      LAC = other.LAC
      totalBytes = other.totalBytes
      upBytes = other.upBytes
      downBytes = other.downBytes
      transTime = other.transTime
      avgDownSpeed = other.avgDownSpeed
      avgUpSpeed = other.avgUpSpeed
    }

  }

  def reduceFrom(other: PSNetLACRecord): Unit = {
    this.upBytes += other.upBytes
    this.downBytes += other.downBytes
    this.transTime += other.transTime
  }

  def calculate(): Unit = {
    this.avgDownSpeed = downBytes.toDouble / transTime
    this.avgUpSpeed = upBytes.toDouble / transTime
    this.totalBytes = upBytes + downBytes
  }

  def getKey(): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(firstTime).append("_").append(LAC)
    sb.toString()
  }


  override def toString(): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(firstTime).append(",").append(LAC).append(",").append(upBytes.toString).append(",")
      .append(downBytes.toString).append(",").append(transTime.toString)
      .append(",").append(avgDownSpeed).append(",").append(avgUpSpeed)
    sb.toString()
  }

}
