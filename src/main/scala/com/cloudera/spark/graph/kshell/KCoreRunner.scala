package com.cloudera.spark.graph.kshell

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, _}

/**
 * Created by root on 4/22/15.
 */
object KCoreRunner extends Logging {

  def run(sc: SparkContext, cdrData: RDD[(String, String)], k: Int, kmin: Int, top: Int): Unit = {

    val vertex = cdrData.flatMap(line => {
      Seq(line._1, line._2)
    }).distinct().zipWithUniqueId().cache()

//    val cdrIdData = cdrData.join(vertex).map { case (caller, (callee, callerId)) => (callee, callerId) }.join(vertex).map { case (callee, (callerId, calleeId)) => (callerId, calleeId) }

    val cdrIdData = cdrData.map{
      case (caller, callee) => ((caller, callee), 1)
    }.reduceByKey((a, b) => a+b).map{
      case ((caller, callee), v) => {
        (caller, (callee, v))
      }
    }.join(vertex).map {
      case (caller, ((callee, v), callerId)) => (callee, (callerId, v))
    }.join(vertex).map {
      case (callee, ((callerId, v), calleeId)) => (callerId, calleeId, v)
    }

    val edges = cdrIdData.map{
      case (callerId, calleeId, v) => {
        Edge(callerId, calleeId, v)
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    println("Before: V=" + vertex.count() + " E=" + edges.count())

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
    var graph = Graph[String, Int](vertex.map { case (value, id) => (id, value) }, edges, defaultUser)
      .partitionBy(PartitionStrategy.RandomVertexCut)
      .groupEdges((a, b) => {
      a + b
    })
    //    graph.triplets.map(edge => {
    //      (edge.srcAttr, edge.dstAttr, edge.attr)
    //    }).collect.foreach(println)

    var (retK, finalRet, beforeFinal) = KCoreFast.run(graph, k, kmin, top)

    //    ret.partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => {a+b}).triplets.filter{t => t.srcAttr >= k && t.dstAttr >= k }.collect.foreach(println)
    println("Output final result")
    finalRet.vertices.filter { case (vid, vd) => vd >= 0 }.collect().foreach(println)
    var vCount = finalRet.vertices.filter { case (vid, vd) => vd >= 0 }.count()
    var eCount = finalRet.triplets.filter { t => t.srcAttr >= 0 && t.dstAttr >= 0 }.count()
    logWarning(s"K=$retK, V=$vCount, E=$eCount")

    println("Output result last interation")
    beforeFinal.vertices.filter { case (vid, vd) => vd >= 0 }.collect().foreach(println)
    var vCount2 = beforeFinal.vertices.filter { case (vid, vd) => vd >= 0 }.count()
    var eCount2 = beforeFinal.triplets.filter { t => t.srcAttr >= 0 && t.dstAttr >= 0 }.count()
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

}
