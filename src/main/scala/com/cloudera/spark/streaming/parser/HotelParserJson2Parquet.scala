package com.cloudera.spark.streaming.parser

import com.amazonaws.util.json.JSONObject
import com.cloudera.spark.streaming.ParseException

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/11/15.
 */
object HotelParserJson2Parquet {

  val OPS = "upsert"
  val TABLE_NAME = "table";
  val COLUMN_FAMILY = "cf";
  val KEY_VALUE = "kvs";
  val KEY = "key";
  val VALUE = "values";



  //  val COLUMN_FAMILY = "common".getBytes
  val COLUMN_QUALIFERS = Array(
    "LKDJBM",
    "QYJBQYMC",
    "LKDJXM",
    "XBDM",
    "LKDJCSRQ",
    "GJDM",
    "RYLXDM",
    "MZDM",
    "ZJLXDM",
    "LKDJZJHM",
    "DZQHDM",
    "LKDJZJXZ",
    "LKDJRZSJ",
    "LKDJRZFH",
    "LKDJCSSJ",
    "LKDJTFSJ",
    "GABMDM",
    "GABM_CLASS_LINK_MIXED_BM"
  )

  @Override
  def parse(line: String) : Map[String, ArrayBuffer[HotelTrans]] = {
    try {
      var json = new JSONObject(line)
      //    var jsonArray = json.getJSONArray(OPS)
      var oneUpsert = json.getJSONObject(OPS)
      var ret: Map[String, ArrayBuffer[HotelTrans]] = Map[String, ArrayBuffer[HotelTrans]]()

      //    for(i <- 0 until jsonArray.length){
      //      var oneUpsert = jsonArray.get(i).asInstanceOf[JSONObject]
      var table = oneUpsert.getString(TABLE_NAME)
      var cf = oneUpsert.getString(COLUMN_FAMILY).getBytes()
      if (!ret.contains(table)) {
        ret += (table -> new ArrayBuffer[HotelTrans]())
      }
      var records: ArrayBuffer[HotelTrans] = ret(table)

      var kvs = oneUpsert.getJSONArray(KEY_VALUE)

      for (j <- 0 until kvs.length()) {
        var kv = kvs.get(j).asInstanceOf[JSONObject]

        var values = kv.getJSONObject(VALUE)

//        println(values.getString("LKDJBM"))

        var oneRecord: HotelTrans = new HotelTrans(
          values.getString("LKDJBM"),
          values.getString("QYJBQYMC"),
          values.getString("LKDJXM"),
          values.getString("XBDM"),
          values.getString("LKDJCSRQ"),
          values.getString("GJDM"),
          values.getString("RYLXDM"),
          values.getString("MZDM"),
          values.getString("ZJLXDM"),
          values.getString("LKDJZJHM"),
          values.getString("DZQHDM"),
          values.getString("LKDJZJXZ"),
          values.getString("LKDJRZSJ"),
          values.getString("LKDJRZFH"),
          values.getString("LKDJCSSJ"),
          values.getString("LKDJTFSJ"),
          values.getString("GABMDM"),
          values.getString("GABM_CLASS_LINK_MIXED_BM")
        )

        records.+=(oneRecord)
      }
      //    }
      ret
    } catch {
      case ex : Exception => {
        throw new ParseException("Failed to parse " + line, ex);
      }
    }
  }

  case class HotelTrans(
                         LKDJBM: String,
                         QYJBQYMC: String,
                         LKDJXM: String,
                         XBDM: String,
                         LKDJCSRQ: String,
                         GJDM: String,
                         RYLXDM: String,
                         MZDM: String,
                         ZJLXDM: String,
                         LKDJZJHM: String,
                         DZQHDM: String,
                         LKDJZJXZ: String,
                         LKDJRZSJ: String,
                         LKDJRZFH: String,
                         LKDJCSSJ: String,
                         LKDJTFSJ: String,
                         GABMDM: String,
                         GABM_CLASS_LINK_MIXED_BM: String
                         )
}
