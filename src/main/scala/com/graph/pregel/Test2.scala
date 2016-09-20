package com.graph.pregel

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 找到一跳节点和二跳节点
 * Created by lichangyue on 2016/9/20.
 */
object Test2 {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    // 1 2
    // 2 3
    // 1 4
    // 3 5
    // 2 8
    // 8 7
    // 4 5
    // 5 6
    // 6 7
    // 3 7
    // 4 3
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/pregel-02.txt").cache()


    println("\n\n confirm vertices internal of graph")
    graph.vertices.collect.foreach(println(_))
    // (4,1)
    // (6,1)
    // (8,1)
    // (2,1)
    // (1,1)
    // (3,1)
    // (7,1)
    // (5,1)

    println("\n\n confirm edges internal of graph")
    graph.edges.collect.foreach(println(_))
    // Edge(1,2,1)
    // Edge(1,4,1)
    // Edge(2,3,1)
    // Edge(2,8,1)
    // Edge(3,5,1)
    // Edge(8,7,1)
    // Edge(3,7,1)
    // Edge(4,3,1)
    // Edge(4,5,1)
    // Edge(5,6,1)
    // Edge(6,7,1)


    //sendMsg函数
    def sendMsgFunc(edge:EdgeTriplet[Int,Int]) = {
      if(edge.srcAttr <= 0){
        // 如果双方都小于0，则不发送信息
          if(edge.dstAttr <= 0){
            Iterator.empty
          }else{
            // srcAttr小于0，dstAttr大于零，则将dstAttr-1后发送
            Iterator((edge.srcId,edge.dstAttr -1))
          }
      }else{
        if(edge.dstAttr <=0){
          // srcAttr大于0，dstAttr<=0,则将srcAttr-1后发送
          Iterator((edge.dstId,edge.srcAttr -1))
        }else{
          // 双方都大于零，则将属性-1后发送
          val toSrc = Iterator((edge.srcId,edge.dstAttr -1))
          val toDst = Iterator((edge.dstId,edge.srcAttr -1))
          toDst ++ toSrc
        }
      }
    }

    val friends  =Pregel (
      graph.mapVertices((vid,value) => if(vid ==1)2 else -1),
      // 发送初始值
      -1,
      // 指定阶数
      2,
      // 双方向发送
      EdgeDirection.Either
    )(
      vprog =(vid ,attr,msg)=>math.max(attr,msg),
      sendMsgFunc,
      (a,b) => math.max(a,b)
      ).subgraph(vpred = (vid,v) => v >=0)


    println("\n\nconfrim vertices of friends")
    friends.vertices.collect().foreach(println(_))
//    (4,1)
//    (1,2)
//    (3,0)
//    (8,0)
//    (5,0)
//    (2,1)

    sc.stop()

  }

}
