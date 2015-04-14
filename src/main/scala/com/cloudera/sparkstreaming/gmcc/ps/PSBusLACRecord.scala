package com.cloudera.sparkstreaming.gmcc.ps

import java.io.Serializable

/**
 * Created by waller on 3/19/15.
 */
class PSBusLACRecord extends Serializable {

  var firstTime: String = null
  var LAC: String = null
  var serviceGroup: String = null
  var totalBytes: Long = 0
  var imsi: String = null
  var bytePrecent: Double = 0
  var distinctIMSI: Long = 0

  def this(@transient in: Array[String]) = {
    this()
    firstTime = UserServiceParseUtil.clusterTime(in(UserServiceParseUtil.FIRST_TIME))
    LAC = in(UserServiceParseUtil.LAC)
    serviceGroup = in(UserServiceParseUtil.SERVICE_GROUP)
    imsi = in(UserServiceParseUtil.IMSI)
    val upBytes = in(UserServiceParseUtil.UP_BYTES).toLong
    val downBytes = in(UserServiceParseUtil.DOWN_BYTES).toLong
    totalBytes = upBytes + downBytes
  }

  def reduce(other: PSBusLACRecord): PSBusLACRecord = {
    val ret = new PSBusLACRecord(this)
    ret.reduceFrom(other)
    ret
  }

  def this(other: PSBusLACRecord) {
    this()
    if (other == null) {
      firstTime = null
      LAC = null
      serviceGroup = null
      totalBytes = 0
      imsi = null

    } else {
      firstTime = other.firstTime
      LAC = other.LAC
      serviceGroup = other.serviceGroup
      totalBytes = other.totalBytes
      imsi = other.imsi
    }

  }

  def reduceFrom(other: PSBusLACRecord): Unit = {
    this.totalBytes += other.totalBytes
  }

  def calculate(): Unit = {

  }

  def getKey(): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(firstTime).append("_").append(LAC).append("_").append(serviceGroup)
    sb.toString()
  }

  override def toString(): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(firstTime).append(",").append(LAC + serviceGroup).append(",")
      .append(totalBytes.toString).append(",").append(bytePrecent).append(",").append(distinctIMSI)
    sb.toString()
  }

}
