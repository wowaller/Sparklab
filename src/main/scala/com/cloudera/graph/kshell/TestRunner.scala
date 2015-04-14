package com.cloudera.graph.kshell

import com.cloudera.graph.kshell.KCoreFast._
import org.apache.spark._
import org.apache.spark.graphx.{PartitionStrategy, Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD

/**
 * Created by root on 4/9/15.
 */
object TestRunner extends Logging {

  def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("Graphx Test").
      set("spark.cleaner.ttl", "120000")

    //These two lines will get us out SparkContext and our StreamingContext.
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)
    // Create an RDD for the vertices

    var k = 0
    var input: String = null
    var kmin: Int = 1
    var top = -1
    if (args.length < 2) {
      printUsage()
      System.exit(0)
    } else if (args.length == 2) {
      input = args(0)
      k = args(1).toInt
      top = -1
    } else if (args.length == 4) {
      input = args(0)
      k = args(1).toInt
      kmin = args(2).toInt
      top = args(3).toInt
    }

    val cdrData = sc.textFile(args(0)).map(line => {
      var splited = line.split("\\|")
      (splited(0).trim.toLong, splited(2).trim.toLong)
    })

    val edges = cdrData.map(line => {
      Edge(line._1, line._2, 1)
    })

    val vertex = cdrData.flatMap(line => {
      Seq(line._1, line._2)
    }).distinct().map(id => {
      (id, id.toString)
    })

//    val users: RDD[(VertexId, String)] =
//      sc.parallelize(Array((3L, ("a")), (7L, ("b")),
//        (5L, ("c")), (2L, ("d"))))
//
//    // Create an RDD for edges
//    val relationships: RDD[Edge[Int]] =
//      sc.parallelize(Array(Edge(3L, 7L, 1),    Edge(5L, 3L, 1),
//        Edge(2L, 5L, 1), Edge(5L, 7L, 1), Edge(3L, 7L, 1)))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("nobady")

    // Build the initial Graph
    var graph = Graph[String, Int](vertex, edges, defaultUser).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => {a+b})
//    graph.triplets.map(edge => {
//      (edge.srcAttr, edge.dstAttr, edge.attr)
//    }).collect.foreach(println)

    var (retK, finalRet, beforeFinal) = WeightedKCoreFast.run(graph, k, kmin, top)

//    ret.partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => {a+b}).triplets.filter{t => t.srcAttr >= k && t.dstAttr >= k }.collect.foreach(println)
    println("Output final result")
    finalRet.vertices.filter{ case (vid, vd) => vd >= 0}.collect().foreach(println)
    var vCount = finalRet.vertices.filter{ case (vid, vd) => vd >= 0}.count()
    var eCount = finalRet.triplets.filter{t => t.srcAttr >= 0 && t.dstAttr >= 0 }.count()
    logWarning(s"K=$retK, V=$vCount, E=$eCount")

    println("Output result last interation")
    beforeFinal.vertices.filter{ case (vid, vd) => vd >= 0}.collect().foreach(println)
    var vCount2 = beforeFinal.vertices.filter{ case (vid, vd) => vd >= 0}.count()
    var eCount2 = beforeFinal.triplets.filter{t => t.srcAttr >= 0 && t.dstAttr >= 0 }.count()
    logWarning(s"K=$retK, V=$vCount2, E=$eCount2")

    println("Output Report iter " + retK)
    println("Final: V=" + vCount + " E=" + eCount)
    println("Last before Final: V=" + vCount2 + " E=" + eCount2)

//    var ret = WeightedKCoreFast.run(graph, k, 0, top)
//
//    //    ret.partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => {a+b}).triplets.filter{t => t.srcAttr >= k && t.dstAttr >= k }.collect.foreach(println)
//    println("Output final result")
//    ret._1.vertices.filter{ case (vid, vd) => vd >= 1}.collect().foreach(println)
//    var vCount = ret._1.vertices.filter{ case (vid, vd) => vd >= 1}.count()
//    var eCount = ret._1.triplets.filter{t => t.srcAttr >= 1 && t.dstAttr >= 1 }.count()
//    logWarning(s"K=$k, V=$vCount, E=$eCount")
//
//    println("Output result last interation")
//    ret._2.vertices.filter{ case (vid, vd) => vd >= 1}.collect().foreach(println)
//    vCount = ret._2.vertices.filter{ case (vid, vd) => vd >= 1}.count()
//    eCount = ret._2.triplets.filter{t => t.srcAttr >= 1 && t.dstAttr >= 1 }.count()
//    logWarning(s"K=$k, V=$vCount, E=$eCount")

  }

  def printUsage(): Unit = {
    println("Usage: cmd <input> <k>")
  }
}
