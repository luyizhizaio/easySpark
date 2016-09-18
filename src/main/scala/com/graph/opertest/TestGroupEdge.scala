package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/14.
 */
object TestGroupEdge {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);



    val conf = new SparkConf().setMaster("local").setAppName("mask")

    val sc = new SparkContext(conf)

    val users = sc.parallelize(
      Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships = sc.parallelize(
      Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")


    val graph1 = Graph(users,relationships,defaultUser)



    val users2 = sc.parallelize(
      Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (4L, ("franklin", "prof")), (1L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships2 = sc.parallelize(
      Array(Edge(3L, 7L, "collab"),    Edge(4L, 1L, "advisor"),
        Edge(1L, 5L, "colleague"), Edge(4L, 7L, "pi")))


    val graph2 = Graph(users2,relationships2)








  }

}
