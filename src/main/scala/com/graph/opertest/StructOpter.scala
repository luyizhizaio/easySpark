package com.graph.opertest

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/14.
 */
object StructOpter {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

    // day09-vertices.csv
    // 1,Taro,100
    // 2,Jiro,200
    // 3,Sabo,300
    val vertexLines :RDD[String] = sc.textFile("file/data/graph/subgraph-vertices.csv")

    val v: RDD[(VertexId,(String,Long))] = vertexLines.map(line => {
      val cols = line.split(",")
      (cols(0).toLong,(cols(1),cols(2).trim.toLong))
    })

        val format = new SimpleDateFormat("yyyy/MM/dd")
        // day09-01-edges.csv
        // 1,2,100,2014/12/1
        // 2,3,200,2014/12/2
        // 3,1,300,2014/12/3
        /* val edgeLines = sc.textFile("hdfs://S7SA053:8020/stat/subgraph-edges.csv")

         val e = edgeLines.map(line=>{
           val cols =line.split(",")
           Edge(cols(0).toLong,cols(1).toLong,(cols(2).trim.toLong,format.parse(cols(3).trim)))
         })
         //创建图
         val graph = Graph(v,e)

         println("\n\nconfirm Vertices Internal of graph")
         graph.vertices.collect.foreach(println(_))
     //  (1,(Taro,100))
     //  (3,(Sabo,300))
     //  (2,(Jiro,200))

         println("\n\nconfirm edge internal of graph")
         graph.edges.collect.foreach(println(_))
     //    Edge(1,2,(100,Mon Dec 01 00:00:00 CST 2014))
     //    Edge(2,3,(200,Tue Dec 02 00:00:00 CST 2014))
     //    Edge(3,1,(300,Wed Dec 03 00:00:00 CST 2014))

         //1.reverse  操作:边的方向改变了
         println("\n\nconfirm edges reversed graph")
         graph.reverse.edges.collect.foreach(println)
     //    Edge(1,3,(300,Wed Dec 03 00:00:00 CST 2014))
     //    Edge(2,1,(100,Mon Dec 01 00:00:00 CST 2014))
     //    Edge(3,2,(200,Tue Dec 02 00:00:00 CST 2014))

         /**2.subgraph*/
         println("\n\nconfirm subgraphed vertices graph ")
         //根据顶点条件建立子图
         graph.subgraph(vpred =(vid,v) => v._2 >= 200).vertices.collect.foreach(println(_))
     //    (3,(Sabo,300))
     //    (2,(Jiro,200))

         println("\n\n confrim subgraph edges graph")
         //根据边条件建立子图
         graph.subgraph(epred =edge => edge.attr._1 >=200).edges.collect.foreach(println(_))


         //顶点和边同时加限制
         val subgraph = graph.subgraph(
           vpred=(vid,v) => v._2 >=200 ,
           epred =edge => edge.attr._1 >= 200)

         println("\n\n顶点和边限制")
         subgraph.edges.collect.foreach(println(_))
     //    Edge(2,3,(200,Tue Dec 02 00:00:00 CST 2014))
         //3.mask
         val maskedgraph = graph.mask(subgraph)

         println("\nmask 操作")
         //返回一个子图，两个图的交集
         maskedgraph.vertices.collect.foreach(println(_))
     //    (3,(Sabo,300))
     //    (2,(Jiro,200))
         maskedgraph.edges.collect.foreach(println(_))
     //    Edge(2,3,(200,Tue Dec 02 00:00:00 CST 2014))
     */
    //4.groupEdge
    // day09-02-edges.csv
    // 1,2,100,2014/12/1
    // 1,2,110,2014/12/11
    // 2,3,200,2014/12/21
    // 2,3,210,2014/12/2
    // 3,1,300,2014/12/3
    // 3,1,310,2014/12/31
    val edgeLines2 = sc.textFile("file/data/graph/edgegroup.csv")
    val e2 = edgeLines2.map(line =>{
      val cols = line.split(",")
      Edge(cols(0).toLong,cols(1).toLong,(cols(2).trim.toLong,format.parse(cols(3))))
    })

    //构建第二个图
    val graph2 = Graph(v,e2)

    //使用groupEdges将相同边进行合并,e1和e2是需要合并的两条边的属性
    val edgeGroupedGraph = graph2.groupEdges(
      merge =(e1,e2) => (e1._1 + e2._1,if(e1._2.getTime < e2._2.getTime) e1._2 else e2._2) )

    println("\n\nconfirm merged edges graph")
    edgeGroupedGraph.edges.collect.foreach(println)
//    Edge(1,2,(210,Mon Dec 01 00:00:00 CST 2014))
//    Edge(2,3,(410,Tue Dec 02 00:00:00 CST 2014))
//    Edge(3,1,(610,Wed Dec 03 00:00:00 CST 2014))

    sc.stop()
  }

}
