package com.cloudera.spark.streaming.gmcc.ps

/**
 * Created by waller on 3/19/15.
 */
object PSNetLACRecordUtil {
  val FIRST_TIME: Int = 0

  val UP_BYTES = 1

  val DOWN_BYTES = 2

  val TRANS_TIME = 3

  def extract(in: Array[String]): (String, String, Long, Long, Double) = {
    (in(UserServiceParseUtil.FIRST_TIME), in(UserServiceParseUtil.LAC), in(UserServiceParseUtil.UP_BYTES).toLong,
      in(UserServiceParseUtil.DOWN_BYTES).toLong, in(UserServiceParseUtil.TRANS_TIME).toDouble)
  }


}
