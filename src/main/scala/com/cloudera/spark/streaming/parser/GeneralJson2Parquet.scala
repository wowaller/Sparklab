package com.cloudera.spark.streaming.parser

import com.amazonaws.util.json.JSONObject
import com.cloudera.spark.streaming.ParseException
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/11/15.
 */
object GeneralJson2Parquet {

  val OPS = "upsert"
  val TABLE_NAME = "table";
  val COLUMN_FAMILY = "cf";
  val KEY_VALUE = "kvs";
  val KEY = "key";
  val VALUE = "values";

  def parseSchema(schema: String) : Array[String] = {
    schema.split(",")
  }

  @Override
  def parse(line: String, schema: Array[String]) : Map[String, ArrayBuffer[Row]] = {
    try {
      var json = new JSONObject(line)
      //    var jsonArray = json.getJSONArray(OPS)
      var oneUpsert = json.getJSONObject(OPS)
      var ret: Map[String, ArrayBuffer[Row]] = Map[String, ArrayBuffer[Row]]()

      //    for(i <- 0 until jsonArray.length){
      //      var oneUpsert = jsonArray.get(i).asInstanceOf[JSONObject]
      var table = oneUpsert.getString(TABLE_NAME)
      var cf = oneUpsert.getString(COLUMN_FAMILY).getBytes()
      if (!ret.contains(table)) {
        ret += (table -> new ArrayBuffer[Row]())
      }
      var records: ArrayBuffer[Row] = ret(table)

      var kvs = oneUpsert.getJSONArray(KEY_VALUE)

      for (j <- 0 until kvs.length()) {
        var kv = kvs.get(j).asInstanceOf[JSONObject]

        var values = kv.getJSONObject(VALUE)

//        println(values.getString("LKDJBM"))

        val rowElements = new Array[String](schema.length)

        for (i <- 0 until rowElements.length) {
          rowElements(i) = values.getString(schema(i))
        }

        var oneRecord: Row = Row.fromSeq(rowElements)

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
}
