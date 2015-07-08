package com.cloudera.spark.streaming.parser

import com.amazonaws.util.json.JSONObject
import com.cloudera.spark.streaming.ParseException
import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 6/11/15.
 */
object GeneralHBaseJsonParser {

  val OPS = "upsert"
  val TABLE_NAME = "table";
  val COLUMN_FAMILY = "cf";
  val KEY_VALUE = "kvs";
  val KEY = "key";
  val VALUE = "values";

  @Override
  def parse(line: String) : Map[String, ArrayBuffer[Put]] = {
    try {
      var json = new JSONObject(line)
      //    var jsonArray = json.getJSONArray(OPS)
      var oneUpsert = json.getJSONObject(OPS)
      var ret: Map[String, ArrayBuffer[Put]] = Map[String, ArrayBuffer[Put]]()

      //    for(i <- 0 until jsonArray.length){
      //      var oneUpsert = jsonArray.get(i).asInstanceOf[JSONObject]
      var table = oneUpsert.getString(TABLE_NAME)
      var cf = oneUpsert.getString(COLUMN_FAMILY).getBytes()
      if (!ret.contains(table)) {
        ret += (table -> new ArrayBuffer[Put]())
      }
      var puts: ArrayBuffer[Put] = ret(table)

      var kvs = oneUpsert.getJSONArray(KEY_VALUE)

      for (j <- 0 until kvs.length()) {
        var kv = kvs.get(j).asInstanceOf[JSONObject]
        var onePut: Put = new Put(kv.getString(KEY).getBytes())
        var values = kv.getJSONObject(VALUE)
        var columns = values.keys()

        while (columns.hasNext) {
          var column = columns.next().asInstanceOf[String]
          onePut.addColumn(cf, column.getBytes(), values.getString(column).getBytes())
        }

        puts.+=(onePut)
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
