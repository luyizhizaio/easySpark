package com.graph.algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/1.
 * 合并多图,用A关系图去扩充B关系图
 *
 */
object CombineMultiGraph {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf  = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[5]")
    val sc = new SparkContext(conf)

//    1 2
//    1 3
//    2 4
//    3 5
//    4 5
//    5 6
    val edgesA = sc.textFile("file/data/graph/combineA.csv").map(x => {
      val arr = x.split(" ")
      Edge(arr(0).toLong,arr(1).toLong, 0.1)
    })
    val graphA = Graph.fromEdges(edgesA,0)

    //    1 2
    //    1 3
    //    3 4
    var edgesB = sc.textFile("file/data/graph/combineB.csv").map(x => {
      val arr = x.split(" ")
      Edge(arr(0).toLong,arr(1).toLong,0.2)
    })
    val graphB = Graph.fromEdges(edgesB,1)

    //使用outerJoinVertices将user中的属性赋给graph中的顶点，如果图中顶点不在RDD中，则赋值为None

   /* val graph3 = graph.mapVertices((id,attr)=> "").outerJoinVertices(users){
      (vid,attr,user) => user.getOrElse("None")}*/

    val tmpGraph = graphA.outerJoinVertices(graphB.vertices){(vid,va,vb) => vb.getOrElse(None)}

    tmpGraph.triplets.foreach(println)

//    ((3,1),(5,None),0.1)
//    ((1,1),(2,1),0.1)
//    ((4,1),(5,None),0.1)
//    ((1,1),(3,1),0.1)
//    ((5,None),(6,None),0.1)
//    ((2,1),(4,1),0.1)


    /**
     * //顶点和边同时加限制
    val subgraph = graph.subgraph(
      vpred=(vid,v) => v._2 >=200 ,
      epred =edge => edge.attr._1 >= 200)
     */
    val tmpGraph2 = tmpGraph.subgraph(epred =edge => edge.dstAttr.!=(None) ||edge.srcAttr.!=(None))
    tmpGraph2.triplets.foreach(println)

//    ((3,1),(5,None),0.1)
//    ((1,1),(2,1),0.1)
//    ((4,1),(5,None),0.1)
//    ((1,1),(3,1),0.1)
//    ((2,1),(4,1),0.1)

    // 合并临时图和B图
    val unionEdges = graphB.edges.union(tmpGraph2.edges).map(x => (x.srcId.toString.concat(x.dstId.toString),x)).reduceByKey((e1,e2) =>e1).map(x=>x._2)
    unionEdges.foreach(println(_))
    val graphC = Graph.fromEdges(unionEdges,1)

    println("combine vertices result C graph:")
    graphC.triplets.foreach(println(_))
//    (3,1),(4,1),0.2)
//    ((1,1),(2,1),0.2)
//    ((3,1),(5,1),0.1)
//    ((1,1),(3,1),0.2)
//    ((4,1),(5,1),0.1)
//    ((2,1),(4,1),0.1)

    sc.stop()
  }

}
