package com.cloudera.graph.kshell

import org.apache.spark._
import org.apache.spark.graphx._

import scala.math._
import scala.reflect.ClassTag

object WeightedKCoreFast extends Logging {
  /**
   * Compute the k-core decomposition of the graph for all k <= kmax. This
   * uses the iterative pruning algorithm discussed by Alvarez-Hamelin et al.
   * in K-Core Decomposition: a Tool For the Visualization of Large Scale Networks
   * (see <a href="http://arxiv.org/abs/cs/0504107">http://arxiv.org/abs/cs/0504107</a>).
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   *
   * @param graph the graph for which to compute the connected components
   * @param kmax the maximum value of k to decompose the graph
   *
   * @return a graph where the vertex attribute is the minimum of
   *         kmax or the highest value k for which that vertex was a member of
   *         the k-core.
   *
   * @note This method has the advantage of returning not just a single kcore of the
   *       graph but will yield all the cores for all k in [1, kmax].
   */

  def run[VD: ClassTag](graph: Graph[VD, Int],
                        kmax: Int,
                        kmin: Int = 1,
                        top: Int = -1)
  : (Int, Graph[Int, Int], Graph[Int, Int]) = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => newData.getOrElse(0)).cache()
    var weighted = computeWeightedDegree(g).cache()
    var weightedG = g.outerJoinVertices(weighted.vertices)((vid, oldData, newData) => (oldData, newData.getOrElse(0)))
    var oldG = weightedG
    var vCount = Long.MaxValue
    var eCount = Long.MaxValue
    // val degrees = graph.degrees
    // val numVertices = degrees.count
    // logWarning(s"Numvertices: $numVertices")
    // logWarning(s"degree sample: ${degrees.take(10).mkString(", ")}")
    // logWarning("degree distribution: " + degrees.map{ case (vid,data) => (data, 1)}.reduceByKey((_+_)).collect().mkString(", "))
    // logWarning("degree distribution: " + degrees.map{ case (vid,data) => (data, 1)}.reduceByKey((_+_)).take(10).mkString(", "))
    var curK = kmin - 1
    while (curK < kmax && vCount > top) {
      var kSquare = curK * curK
      oldG.unpersist()
      oldG = weightedG
      weightedG = computeCurrentKCore(weightedG, kSquare).cache
      vCount = weightedG.vertices.filter { case (vid, vd) => vd._2 >= kSquare }.count()
      eCount = weightedG.triplets.filter { t => t.srcAttr._2 >= kSquare && t.dstAttr._2 >= kSquare }.count()
      logWarning(s"K=$curK, V=$vCount, E=$eCount")
      curK += 1
    }
    (curK, weightedG.mapVertices({ case (_, k) => k._2 }), oldG.mapVertices({ case (_, k) => k._2 }))
  }

  def computeWeightedDegree(graph: Graph[Int, Int]) = {
    logWarning(s"Computing weight")

    def sendMsg(et: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
      Iterator((et.srcId, et.srcAttr * et.attr), (et.dstId, et.dstAttr * et.attr))
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: Int, m2: Int): Int = {
      m1 + m2
    }

    def vProg(vid: VertexId, data: Int, update: Int): Int = {
      max(data + update, 0)
    }

    // Note that initial message should have no effect
    Pregel(graph, 0, 1)(vProg, sendMsg, mergeMsg)
    //    graph.pregel(0, 1)(vProg, sendMsg, mergeMsg)
  }

  def computeCurrentKCore(graph: Graph[(Int, Int), Int], k: Int): Graph[(Int, Int), Int] = {
    //    val kSquare = k * k
    logWarning(s"Computing kcore for k=$k")
    def sendMsg(et: EdgeTriplet[(Int, Int), Int]): Iterator[(VertexId, (Int, Int))] = {
      if (et.srcAttr._2 < 0 || et.dstAttr._2 < 0) {
        // if either vertex has already been turned off we do nothing
        Iterator.empty
      } else if (et.srcAttr._2 < k && et.dstAttr._2 < k) {
        // tell both vertices to turn off but don't need change count value
        Iterator((et.srcId, (-1, -1)), (et.dstId, (-1, -1)))

      } else if (et.srcAttr._2 < k) {
        // if src is being pruned, tell dst to subtract from vertex count
        Iterator((et.srcId, (-1, -1)), (et.dstId, (1, et.attr)))

      } else if (et.dstAttr._2 < k) {
        // if dst is being pruned, tell src to subtract from vertex count
        Iterator((et.dstId, (-1, -1)), (et.srcId, (1, et.attr)))

      } else {
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: (Int, Int), m2: (Int, Int)): (Int, Int) = {
      if (m1._1 < 0 || m2._1 < 0) {
        (-1, -1)
      } else {
        (m1._1 + m2._1, m1._2 + m2._2)
      }
    }

    def vProg(vid: VertexId, data: (Int, Int), update: (Int, Int)): (Int, Int) = {
      if (update._1 < 0) {
        // if the vertex has turned off, keep it turned off
        (-1, -1)
      } else if (data._1 <= 0) {
        (-1, -1)
      }
      else {
        // subtract the number of neighbors that have turned off this round from
        // the count of active vertices
        // TODO(crankshaw) can we ever have the case data < update?
        var newDegree = data._1 - update._1
        (newDegree, (data._2 / data._1 - update._2) * newDegree)
      }
    }

    // Note that initial message should have no effect
    Pregel(graph, (0, 0))(vProg, sendMsg, mergeMsg)
  }
}
