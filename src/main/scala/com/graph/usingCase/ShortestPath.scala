package com.graph.usingCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, VertexId, Graph}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by lichangyue on 2016/10/14.
  */
object ShortestPath {


  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("ShortestPath").setMaster("local")
    val sc = new SparkContext(conf)
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)

    graph.edges.foreach(println)
//    Edge(0,1,1.0)
//    Edge(0,2,1.0)

    val sourceId: VertexId = 0 // The ultimate source

    // 设置属性，sourceId=0
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    initialGraph.vertices.foreach(println)
//    (0,(0.0,List(0)))
//    (1,(Infinity,List()))

    println("--------------------")
    val shortestPath = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)


    println(shortestPath.vertices.collect.mkString("\n"))
  }

 }
