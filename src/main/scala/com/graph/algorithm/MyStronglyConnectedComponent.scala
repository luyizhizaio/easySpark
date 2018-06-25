package com.graph.algorithm

/**
 * Created by lichangyue on 2016/12/21.
 */
import scala.reflect.ClassTag

import org.apache.spark.graphx._
object MyStronglyConnectedComponent {


    /**
     * Compute the strongly connected component (SCC) of each vertex and return a graph with the
     * vertex value containing the lowest vertex id in the SCC containing that vertex.
     *
     * @tparam VD the vertex attribute type (discarded in the computation)
     * @tparam ED the edge attribute type (preserved in the computation)
     *
     * @param graph the graph for which to compute the SCC
     *
     * @return a graph with vertex attributes containing the smallest vertex id in each SCC
     */
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[VertexId, ED] = {
      require(numIter > 0, s"Number of iterations must be greater than 0," +
        s" but got ${numIter}")

      // the graph we update with final SCC ids, and the graph we return at the end
      var sccGraph = graph.mapVertices { case (vid, _) => vid } //使用vid更新顶点属性
      println("sccGraph:")
      sccGraph.vertices.foreach(println)

      // graph we are going to work with in our iterations
      var sccWorkGraph = graph.mapVertices { case (vid, _) => (vid, false) }.cache()

      println("sccWorkGraph:")
      sccWorkGraph.vertices.foreach(println)

      var numVertices = sccWorkGraph.numVertices //顶点数量
      var iter = 0
      while (sccWorkGraph.numVertices > 0 && iter < numIter) {
        iter += 1
        do {
          numVertices = sccWorkGraph.numVertices //更新顶点数量

          println("iter："+iter+",sccWorkGraph.numVertices:"+numVertices)

          sccWorkGraph = sccWorkGraph.outerJoinVertices(sccWorkGraph.outDegrees) {
            (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true) //没有出度的点 设置为true
          }.outerJoinVertices(sccWorkGraph.inDegrees) {
            (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)//没有入度的点 设置为true
          }.cache()

          println("iter："+iter+",sccWorkGraph:")
          sccWorkGraph.vertices.foreach(println)

          // 保留只有出度或入度的点
          val finalVertices = sccWorkGraph.vertices
            .filter { case (vid, (scc, isFinal)) => isFinal}
            .mapValues { (vid, data) => data._1}//更新点属性为顶点id

          finalVertices.foreach(x => println("finalVertices:"+x))

          // write values to sccGraph
          sccGraph = sccGraph.outerJoinVertices(finalVertices) {
            (vid, scc, opt) => opt.getOrElse(scc) //
          }

          println("iter："+iter+",sccGraph:")
          sccGraph.vertices.foreach(println)

          // 去除第二个值为true的顶点
          sccWorkGraph = sccWorkGraph.subgraph(vpred = (vid, data) => !data._2).cache()
        } while (sccWorkGraph.numVertices < numVertices)

        sccWorkGraph = sccWorkGraph.mapVertices{ case (vid, (color, isFinal)) => (vid, isFinal) }
        println("iter："+iter+",sccWorkGraph:")
        sccWorkGraph.vertices.foreach(println) //
        // collect min of all my neighbor's scc values, update if it's smaller than mine
        // then notify any neighbors with scc values larger than mine
        sccWorkGraph = Pregel[(VertexId, Boolean), ED, VertexId](
          sccWorkGraph, Long.MaxValue, activeDirection = EdgeDirection.Out)(
            (vid, myScc, neighborScc) => (math.min(myScc._1, neighborScc), myScc._2),
            e => {
              if (e.srcAttr._1 < e.dstAttr._1) { //判断源点小于目标点才发送数据，
                Iterator((e.dstId, e.srcAttr._1)) //向目标id发送srcAttr._1
              } else {
                Iterator()
              }
            },
            (vid1, vid2) => math.min(vid1, vid2))//合并顶点的消息，取消的一个

        println("iter："+iter+",sccWorkGraph:")
        sccWorkGraph.vertices.foreach(println)

        // start at root of SCCs. Traverse values in reverse, notify all my neighbors
        // do not propagate if colors do not match!
        sccWorkGraph = Pregel[(VertexId, Boolean), ED, Boolean](
          sccWorkGraph, false, activeDirection = EdgeDirection.In)(
            // vertex is final if it is the root of a color
            // or it has the same color as a neighbor that is final
            (vid, myScc, existsSameColorFinalNeighbor) => {
              val isColorRoot = vid == myScc._1
              (myScc._1, myScc._2 || isColorRoot || existsSameColorFinalNeighbor)
            },
            // activate neighbor if they are not final, you are, and you have the same color
            e => {
              val sameColor = e.dstAttr._1 == e.srcAttr._1
              val onlyDstIsFinal = e.dstAttr._2 && !e.srcAttr._2
              if (sameColor && onlyDstIsFinal) {
                Iterator((e.srcId, e.dstAttr._2))
              } else {
                Iterator()
              }
            },
            (final1, final2) => final1 || final2)
            println("iter："+iter+",sccWorkGraph:")
            sccWorkGraph.vertices.foreach(println)

      }
      sccGraph
    }

}
