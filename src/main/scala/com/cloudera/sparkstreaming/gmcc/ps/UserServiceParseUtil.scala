package com.cloudera.sparkstreaming.gmcc.ps

import com.cloudera.gmcc.test.io.{FileRowTextInputFormat, FileRowWritable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by waller on 3/18/15.
 */
object UserServiceParseUtil {

  val CREATE_TIME = 0

  val FIRST_TIME = 1

  val END_TIME = 2

  val CALLING = 3

  val IMSI = 4

  val USER_IP = 5

  val IMEI = 6

  val MCC = 7

  val MNC = 8

  val LAC = 9

  val RAC = 10

  val CELL_ID = 11

  val SGSN_SG_IP = 12

  val GGSN_SG_IP = 13

  val SGSN_DATA_IP = 14

  val GGSN_DATA_IP = 15

  val APN = 16

  val RAT = 17

  val SERVICE_TYPE = 18

  val SERVICE_GROUP = 19

  val UP_PACKETS = 20

  val DOWN_PACKETS = 21

  val UP_BYTES = 22

  val DOWN_BYTES = 23

  val UP_SPEED = 24

  val DOWN_SPEED = 25

  val TRANS_TIME = 26

  val IS_END = 27

  val USER_PORT = 28

  val PROTO_TYPE = 29

  val DEST_IP = 30

  val DEST_PORT = 31

  val CDR_ID = 32

  val SERVICE_TYPE_SDK = 33

  val SERVICE_GROUP_SDK = 34

  val IS_HTTP = 35

  def HDFSFileSource(ssc: StreamingContext, inputPath: String): DStream[Array[String]] = {
    //    LOG.info("Get inputPath as: " + inputPath)
    //Simple example of how you set up a receiver from a HDFS folder
    ssc.fileStream[FileRowWritable, Text, FileRowTextInputFormat](inputPath, (t: Path) => {
      if (t.getName.endsWith("_COPYING_")) {
        false
      } else {
        //        if (deleteOnComplete) {
        //          files += t
        //        }
        true
      }
    }, true).map(line => new String(line._2.copyBytes(), "utf8").split(SOURCE_DELIMITER))
  }

  def clusterTime(time: String): String = {
    var minIndex = time.indexOf(":") + 1
    var min = time.substring(minIndex, minIndex + 2).toInt
    min = (min / 5) * 5
    time.substring(0, minIndex) + min
//    if (min >= 0 && min < 5) {
//      time.substring(0, minIndex) + "00"
//    } else if (min < 10)
  }

  val SOURCE_DELIMITER = ","
}
