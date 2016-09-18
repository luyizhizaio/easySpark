package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/14.
 */
object TestMask {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName("mask")

    val sc = new SparkContext(conf)

    val users = sc.parallelize(
      Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
      (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships = sc.parallelize(
      Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")


    val graph = Graph(users,relationships,defaultUser)

    println("graph:")
    graph.triplets.map(
      triplet => " srcid:"+triplet.srcId +", dstID:" + triplet.dstId +",srcAttr：" + triplet.srcAttr + " ，attr： " + triplet.attr +" ，dstAttr： " + triplet.dstAttr
    ).collect().foreach(println(_))


    //connectedComponents源码：返回连接成分的顶点值：包含顶点Id，顶点的属性没了
    val ccGraph = graph.connectedComponents()
    println("ccGraph:")
    ccGraph.triplets.map(
    triplet => " srcid:"+triplet.srcId +", dstID:" + triplet.dstId +",srcAttr：" + triplet.srcAttr + " ，attr： " + triplet.attr +" ，dstAttr： " + triplet.dstAttr
    ).collect().foreach(println(_))

    val validGraph = graph.subgraph(vpred = (id,attr) => attr._2 !="Missing")

    println("validGraph:")
    validGraph.triplets.map(
      triplet => " srcid:"+triplet.srcId +", dstID:" + triplet.dstId +",srcAttr：" + triplet.srcAttr + " ，attr： " + triplet.attr +" ，dstAttr： " + triplet.dstAttr
    ).collect().foreach(println(_))


    val validccGraph = ccGraph.mask(validGraph)

    println("validCCGraph:")
    validccGraph.triplets.map(
      triplet => " srcid:"+triplet.srcId +", dstID:" + triplet.dstId +",srcAttr：" + triplet.srcAttr + " ，attr： " + triplet.attr +" ，dstAttr： " + triplet.dstAttr
    ).collect().foreach(println(_))

  }

}
