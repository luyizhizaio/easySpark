package com.graph.algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/19.
 */
object TestConnectedComponent {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

    // 1 2
    // 2 3
    // 3 1
    // 4 5
    // 5 6
    // 6 7
    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/cc.txt").cache()
    println("\n\nconfirm vertices internal of graph")
    graph.vertices.collect.foreach(println(_))
    // (4,1)
    // (6,1)
    // (2,1)
    // (1,1)
    // (3,1)
    // (7,1)
    // (5,1)

    println("\n\nconfirm edges internal of graph")
    graph.edges.collect.foreach(println(_))
    // Edge(1,2,1)
    // Edge(2,3,1)
    // Edge(3,1,1)
    // Edge(4,5,1)
    // Edge(5,6,1)
    // Edge(6,7,1)
    // 1,2,3相连，4,5,6,7相连

    // 取连通图，连通图以图中最小Id作为label给图中顶点的属性
    val cc:Graph[Long,Int] = graph.connectedComponents()

    println("\n\nconfirm vertices connected components")
    cc.vertices.collect().foreach(println(_))
//    (4,4) //后面的4是label
//    (1,1)
//    (6,4)
//    (3,1)
//    (7,4)
//    (5,4)
//    (2,1)


    // 取出id为2的顶点的label (顶点的属性)

    val cc_label_of_vid_2:Long = cc.vertices.filter{
      case(id,label) => id ==2
    }.first._2

    println("\n\nconfirm connected componets label of vertex id 2")
    println(cc_label_of_vid_2)
    //1

    // 取出相同类标的顶点 ，类标号为1
    val vertices_connected_with_vid_2:RDD[(Long,Long)]= cc.vertices.filter{
      case(id,label) => label == cc_label_of_vid_2
    }

    println("\n\nconfirm vertices_connected_with_vid_2 ")
    vertices_connected_with_vid_2.collect().foreach(println(_))
    // (2,1)
    // (1,1)
    // (3,1)

    //返回顶点ID
    val vids_connected_with_vid_2:RDD[Long] =vertices_connected_with_vid_2.map(v=>v._1)
    println("\n\nconfirm vids_connected_with_vid_2")
    vids_connected_with_vid_2.collect.foreach(println(_))
    // 2
    // 1
    // 3

    val vids_list:Array[Long] = vids_connected_with_vid_2.collect()

    //取出子图， 判断顶点在vids_list中的子图
    val graphIncludeVid2 =graph.subgraph(vpred = (vid,attr) => vids_list.contains(vid))
    println("\n\nconfirm graphIncludeVid2")
    graphIncludeVid2.vertices.collect().foreach(println(_))
    // (2,1)
    // (1,1)
    // (3,1)

    sc.stop()
  }
}
