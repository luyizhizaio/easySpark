package com.graph.usingCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lichangyue on 2016/10/14.
  */
object ShortestPath2 {


  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("ShortestPath").setMaster("local")
    val sc = new SparkContext(conf)


    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),              //在这里后面的属性("Alice", 28)没有用上。
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)


    val sourceId: VertexId = 5L // 定义源点

    //初始化一个新的图，该图的节点属性为graph中各节点到原点的距离
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    println("\n vertrices:")
    println(initialGraph.vertices.collect.mkString("\n"))
//    (4,Infinity)
//    (1,Infinity)
//    (6,Infinity)
//    (3,Infinity)
//    (5,0.0)
//    (2,Infinity)

    println("\n edges:")
    println(initialGraph.edges.collect.mkString("\n"))
//    Edge(2,1,7)
//    Edge(2,4,2)
//    Edge(3,2,4)
//    Edge(3,6,3)
//    Edge(4,1,1)
//    Edge(5,2,2)
//    Edge(5,3,8)
//    Edge(5,6,3)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类（Double）
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））
      triplet => {  // 计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner
      (a,b) => math.min(a,b) // 最短距离
    )

    println("\n 输出各顶点到5点的距离：")
    println(sssp.vertices.collect.mkString("\n"))
//    (4,4.0)
//    (1,5.0)
//    (6,3.0)
//    (3,8.0)
//    (5,0.0)
//    (2,2.0)

    sc.stop()

  }

 }
