package com.graph.pregel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 在设置传播方向延src到dst时，找到3步之内能形成环路的顶点
 * Created by lichangyue on 2016/9/19.
 */
object Test1 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

    // 0 1
    // 1 2
    // 2 3
    // 3 4
    // 2 4
    // 4 1
    // 4 5
    // 5 6
    // 6 1
    // 5 7
    // 7 8
    val graph:Graph[Int,Int] =GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/pregel-01.txt").cache()

    println("\n\nconfirm vertices internal of graph")
    graph.vertices.collect.foreach(println(_))
    // (4,1)
    // (0,1)
    // (6,1)
    // (8,1)
    // (2,1)
    // (1,1)
    // (3,1)
    // (7,1)
    // (5,1)


    println("\n\nconfirm edges onternal of graph")
    graph.edges.collect.foreach(println(_))
    // Edge(0,1,1)
    // Edge(1,2,1)
    // Edge(2,3,1)
    // Edge(2,4,1)
    // Edge(3,4,1)
    // Edge(4,1,1)
    // Edge(4,5,1)
    // Edge(5,6,1)
    // Edge(5,7,1)
    // Edge(6,1,1)
    // Edge(7,8,1)


    val circleGraph =Pregel(
      // 各顶点置空
      graph.mapVertices((id,attr)=> Set[VertexId]()),
      // 最初值为空
      Set[VertexId](),
      //最大迭代次数为3
      3,
      // 发送消息（默认出边）的方向
      EdgeDirection.Out)(
      // 用户定义的接收消息 ,vprog
      (id,attr,msg) => {
        println("vprog: id:"+id +",attr:"+ attr +",msg:"+ msg)
        (msg ++ attr)
      },
      // 计算消息,sendMsg
      edge => Iterator((edge.dstId,(edge.srcAttr + edge.srcId))), //向集合中添加元素
      // 合并消息,mergerMsg
      (a,b) => (a ++ b) //合并两个集合
      // 取出包含自身id的点
    ).subgraph(vpred =(id,attr) => attr.contains(id))



    println("\n\nconfirm vertices of circleGraph")
    circleGraph.vertices.collect.foreach(println(_))
//    (4,Set(0, 1, 6, 2, 3, 4))
//    (1,Set(0, 5, 1, 6, 2, 3, 4))
//    (2,Set(0, 5, 1, 6, 2, 3, 4))
  }
}
