package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, VertexId, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/18.
 */
object TestDegree {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    // 2 1
    // 3 1
    // 4 1
    // 5 1
    // 1 2
    // 4 3
    // 5 3
    // 1 4
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/degree.csv").cache()

    println("\n\nConfirm vertices Internal of graph")
    graph.vertices.collect().foreach(println(_))
    // (4,1)
    // (2,1)
    // (1,1)
    // (3,1)
    // (5,1)

    println("\n\n confirm edges internal of grahp")

    graph.edges.collect.foreach(println(_))
    // Edge(2,1,1)
    // Edge(3,1,1)
    // Edge(4,1,1)
    // Edge(5,1,1)
    // Edge(1,2,1)
    // Edge(1,4,1)
    // Edge(4,3,1)
    // Edge(5,3,1)

    //1.degree
    println("\n\nconfirm indegrees")
    graph.inDegrees.collect.foreach(d => println(d._1 + "'s inDegree is " + d._2))
    //    4's inDegree is 1
    //    1's inDegree is 4
    //    3's inDegree is 2
    //    2's inDegree is 1

    println("\n\n confirm outDegrees")
    graph.outDegrees.collect.foreach(d => println(d._1 + "'s outDegree is " + d._2))
    //    4's outDegree is 2
    //    1's outDegree is 2
    //    3's outDegree is 1
    //    5's outDegree is 2
    //    2's outDegree is 1

    println("\n\n cofirm degrees")
    graph.degrees.collect.foreach(d => println(d._1 + "'s degree is " + d._2))
    //    4's degree is 3
    //    1's degree is 6
    //    3's degree is 3
    //    5's degree is 2
    //    2's degree is 2


    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("\n\nconfirm max indegrees")
    println(graph.inDegrees.reduce(max))
    // (1,4)

    //2.collectNeighborIds
    println("\n\nconfirm collectNerighber(In)")
    graph.collectNeighborIds(EdgeDirection.In)
      .collect
      .foreach(n => println(n._1 + "'s in nerghbors:" + n._2.mkString(",")))
    // 4's in neighbors : 1
    // 2's in neighbors : 1
    // 1's in neighbors : 2,3,4,5
    // 3's in neighbors : 4,5
    // 5's in neighbors :

    println("\n\nconfirm collectNeighborIds(OUT)")
    graph.collectNeighborIds(EdgeDirection.Out)
      .collect
      .foreach(n => println(n._1 + "'s out neighbors:" + n._2.mkString(",")))
    //    4's out neighbors:1,3
    //    1's out neighbors:2,4
    //    3's out neighbors:1
    //    5's out neighbors:1,3
    //    2's out neighbors:
    println("\n\n~~~~~~~~~ Confirm collectNeighborIds(Either) ")
    graph.collectNeighborIds(EdgeDirection.Either).collect.foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))
    // 4's neighbors : 1,3
    // 2's neighbors : 1
    // 1's neighbors : 2,3,4,5
    // 3's neighbors : 1,4,5
    // 5's neighbors : 1,3


    //3.collectNeighbor~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    println("\n\nconfirm collectNeighbors(IN)")
    graph.collectNeighbors(EdgeDirection.In)
      .collect
      .foreach(n => println(n._1 + "'s in berghbors :" + n._2.mkString(",")))
    //    4's in berghbors :(1,1)
    //    1's in berghbors :(2,1),(3,1),(4,1),(5,1)
    //    3's in berghbors :(4,1),(5,1)
    //    5's in berghbors :
    //    2's in berghbors :(1,1)

    println("\n\n~~~~~~~~~ Confirm collectNeighbors(OUT) ")
    graph.collectNeighbors(EdgeDirection.Out).collect.foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))

    //    4's out neighbors : (1,1),(3,1)
    //    1's out neighbors : (2,1),(4,1)
    //    3's out neighbors : (1,1)
    //    5's out neighbors : (1,1),(3,1)
    //    2's out neighbors : (1,1)

    println("\n\n~~~~~~~~~ Confirm collectNeighbors(Either) ")
    graph.collectNeighbors(EdgeDirection.Either).collect.foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))
    // 4's neighbors : (1,1),(3,1)
    // 2's neighbors : (1,1)
    // 1's neighbors : (2,1),(3,1),(4,1),(5,1)
    // 3's neighbors : (1,1),(4,1),(5,1)
    // 5's neighbors : (1,1),(3,1)

    sc.stop
  }
}
