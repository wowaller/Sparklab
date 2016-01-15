package com.cloudera.spark.bulkload

import scala.collection.mutable

/**
  * Created by root on 1/15/16.
  */
object SplitUtil {

  def split(line: String, delimiter: String): Seq[String] = {
    var last: Int = 0
    var current: Int = 0
    var ret: mutable.MutableList[String] = mutable.MutableList[String]()

    current = line.indexOf(delimiter, last)
    while (current >= 0) {
      ret.+=(line.substring(last, current))
      last = current + 1
      current = line.indexOf(delimiter, last)
    }

    ret.+=(line.substring(last))

    return ret

  }
}
