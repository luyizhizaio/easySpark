package com.graph

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/31.
 */
object FirstGraph {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("graph").setMaster("local")

    val sc = new SparkContext(conf)
    //顶点
    val users = sc.parallelize(
      Array((3L,("rxin","student")),
      (7L,("jgonzal","postdoc")),
      (5L,("franklin","prof")),
      (2L,("istoica","prof"))))
    //边
    val relationships = sc.parallelize(
      Array(Edge(3L,7L,"collab"),Edge(5L,3L,"advisor"),
        Edge(2L,5L,"collab"),Edge(5L,7L,"pi")))

    val defaultUser = ("John Doe","Missing")
    //初始化图
    val graph = Graph(users,relationships,defaultUser)

    // 查询指定职位
    val count = graph.vertices.filter{case (id,(name,pos)) => pos =="postdoc"}.count()
    println("count:"+count)

    //查询边的源id大于目标id
    val edgeCount = graph.edges.filter(e => e.srcId > e.dstId).count()
    println("edgeCount:"+edgeCount)

    //操作triplets
    graph.triplets.collect.foreach( x => {
      println(x.dstAttr +"======="+x.srcAttr)
    })

    //操作顶点
    graph.vertices.foreach(v => {
      println(v._1 + "----" + v._2)
    })

    //边操作
    graph.edges.foreach(e => {
      println("srcid:"+e.srcId +", dstid" +e.dstId)

    })

  }

}
