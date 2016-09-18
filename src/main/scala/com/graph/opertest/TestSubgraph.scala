package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/13.
 */
object TestSubgraph {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("FirstGrahp").setMaster("local")
    val sc = new SparkContext(conf)

    val users = sc.parallelize(
      Array((3L,("rxin","student")),(7L,("jgonzal","postdoc")),
      (5L,("franklin","prof")),(2L,("istoica","prof")),
      (4L,("peter","student"))))

    val relationships = sc.parallelize(
      Array(Edge(3L,7L,"collab"),Edge(5L,3L,"advisor"),
      Edge(2L,5L,"collegaue"),Edge(5L,7L,"pi"),
      Edge(4L,0L,"student"),Edge(5L,0L,"colleague")))

    //
    val defaultUser = ("John Doe","Missing")
    //创建初始图
    val graph =Graph(users,relationships,defaultUser)

    //0号用户被替换成("John Doe","Missing")
    graph.triplets.map(
      triplet => triplet.srcAttr._1 +" is the " + triplet.attr +" of " + triplet.dstAttr
    ).collect.foreach(println(_))

    //移除缺失的顶点和他们连接的边,vpred 是subgraph方法的参数名称
    val validGrap = graph.subgraph(vpred = (id,attr) => attr._2 !="Missing")

    validGrap.vertices.collect.foreach(println(_))

    validGrap.triplets.map(
    triplet=> triplet.srcAttr._1 +" is the "+ triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }
}
