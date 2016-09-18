package com.graph.opertest

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexRDD, EdgeTriplet, Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/18.
 */
object TestPropsOps {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

    // 1,Taro,100
    // 2,Jiro,200
    // 3,Sabo,300
    val vertexLines = sc.textFile("hdfs://S7SA053:8020/stat/subgraph-vertices.csv")
    val vertices = vertexLines.map(line =>{
      val cols = line.split(",")
      (cols(0).trim.toLong,(cols(1).trim,cols(2).trim.toLong))
    })

    val format = new SimpleDateFormat("yyyy/MM/dd")

    // 1,2,100,2014/12/1
    // 2,3,200,2014/12/2
    // 3,1,300,2014/12/3
    val edgesLines = sc.textFile("hdfs://S7SA053:8020/stat/subgraph-edges.csv")

    val edges = edgesLines.map(line =>{
      val cols = line.split(",")
      Edge(cols(0).toLong,cols(1).toLong,(cols(2).toLong,format.parse(cols(3).trim)))
    })


    //生成图
    val graph = Graph(vertices,edges)

    println("\n\nConfirm edges internal of graph")
    graph.edges.foreach(println(_))
    // Edge(1,2,(100,Mon Dec 01 00:00:00 EST 2014))
    // Edge(2,3,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(3,1,(300,Wed Dec 03 00:00:00 EST 2014))



    println("\nconfirm vertices internal of graph")
    graph.vertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (1,(Taro,100))
    // (3,(Sabo,300))



    // 使用mapVertices修改顶点的属性，由原先的(String, Long)修改为（String的length*Long的值）

    val graph2 = graph.mapVertices((vid ,attr) => attr._1.length * attr._2)

    println ("\n\nconfirm vertices internal of graph2")
    graph2.vertices.collect.foreach(println(_))
    // (2,800) Jiro的长度为4，乘以200得到800，下同
    // (1,400)
    // (3,1200)



    // 使用mapEdges将edge的属性由(100,Mon Dec 01 00:00:00 EST 2014)变为100
    val graph3 = graph.mapEdges(edge => edge.attr._1)

    println("\n\nconfirm edges internal of graph3")
    graph3.edges.collect.foreach(println(_))
    // Edge(1,2,100)
    // Edge(2,3,200)
    // Edge(3,1,300)



    println("\n\nconfirm triplets internal of graph")

    graph.triplets.collect.foreach(println(_))
    // ((1,(Taro,100)),(2,(Jiro,200)),(100,Mon Dec 01 00:00:00 EST 2014))
    // ((2,(Jiro,200)),(3,(Sabo,300)),(200,Tue Dec 02 00:00:00 EST 2014))
    // ((3,(Sabo,300)),(1,(Taro,100)),(300,Wed Dec 03 00:00:00 EST 2014))
    // 到这里可以观察到，上述操作对graph本身并没有影响

    // 使用mapTriplets对三元组整体进行操作，即可以利用srcAttr attr dstAttr来修改attr的信息
    val graph4 = graph.mapTriplets(edge => edge.srcAttr._2 + edge.attr._1 + edge.dstAttr._2)
    println("\n\nconfirm veteces internal ")
    graph4.edges.collect.foreach(println(_))
    // Edge(1,2,400) //400 = 100+200+100
    // Edge(2,3,700)
    // Edge(3,1,700)



    // 使用mapReduceTriplets来生成新的VertexRDD
    // 利用map对每一个三元组进行操作
    // 利用reduce对相同Id的顶点属性进行操作
    val newVertices:VertexRDD[Long] = graph.mapReduceTriplets(
      mapFunc = (edge:EdgeTriplet[(String, Long), (Long, java.util.Date)]) => {
        val toSrc = Iterator((edge.srcId, edge.srcAttr._2 - edge.attr._1))
        val toDst = Iterator((edge.dstId, edge.dstAttr._2 + edge.attr._1))
        toSrc ++ toDst
      },
      reduceFunc = (a1:Long, a2:Long) => ( a1 + a2 )
    )

    println("\n\nconfirm vertices internal of newVertices")
    newVertices.collect().foreach(println(_))
    // (2,300)
    // (1,400)
    // (3,500)
  }

}
