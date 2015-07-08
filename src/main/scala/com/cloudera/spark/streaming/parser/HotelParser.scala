package com.cloudera.spark.streaming.parser

import com.cloudera.spark.streaming.ParseException
import org.apache.hadoop.hbase.client.Put

/**
 * Created by root on 6/11/15.
 */
object HotelParser {

  val COLUMN_FAMILY = "common".getBytes
  val COLUMN_QUALIFERS = Array(
    "LKDJXM".getBytes,
    "XBDM".getBytes,
    "LKDJCSRQ".getBytes,
    "GJDM".getBytes,
    "RYLXDM".getBytes,
    "MZDM".getBytes,
    "ZJLXDM".getBytes,
    "LKDJZJHM".getBytes,
    "DZQHDM".getBytes,
    "LKDJZJXZ".getBytes,
    "LKDJRZSJ".getBytes,
    "LKDJRZFH".getBytes,
    "LKDJCSSJ".getBytes,
    "LKDJTFSJ".getBytes,
    "GABMDM".getBytes,
    "GABM_CLASS_LINK_MIXED_BM".getBytes
  )

  @Override
  def parse(key: String, line: String, delimiter : String) : Put = {
    var split = line.split(delimiter)
    if (split.size != COLUMN_QUALIFERS.length) {
      throw new ParseException("Failed to parse line " + line, new Exception());
    }
    var put = new Put(split(0).getBytes())

    for (i <- 0 to COLUMN_QUALIFERS.size) {
      put.addColumn(COLUMN_FAMILY,COLUMN_QUALIFERS(i), split(i).getBytes)
    }

    put
  }
}
