package com.graph

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/18.
 */
object TestVertexRDDAndEdgeRDD {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

    // 1,Taro,100
    // 2,Jiro,200
    // 3,Sabo,300
    val vertexLines = sc.textFile("hdfs://S7SA053:8020/stat/subgraph-vertices.csv")

    val v = vertexLines.map(line =>{
      val cols = line.split(",")
      (cols(0).toLong,(cols(1).trim,cols(2).trim.toLong))
    })

    val format = new SimpleDateFormat("yyyy/MM/dd")

    val edgeLines = sc.textFile("hdfs://S7SA053:8020/stat/subgraph-edges.csv")
    val e = edgeLines.map(line =>{
      val cols = line.split(",")
      Edge(cols(0).toLong, cols(1).toLong,(cols(2).toLong,format.parse(cols(3).trim)))
    })

    val graph = Graph(v,e)

    //返回VertexRDD
    val vertices = graph.vertices

    println("\n\nconfirm vertices internal of graph")
    vertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (1,(Taro,100))
    // (3,(Sabo,300))

    //返回EdgeRDD
    val edges = graph.edges
    println("\n\nconfirm edges internal of graph")
    edges.collect.foreach(println(_))
    // Edge(1,2,(100,Mon Dec 01 00:00:00 EST 2014))
    // Edge(2,3,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(3,1,(300,Wed Dec 03 00:00:00 EST 2014))


    //1.vertexRDD
    // 使用filter筛选出属性value>150的顶点
    val filteredVertices = vertices.filter{
      case (vid:VertexId,(name:String,value:Long)) =>value >150
    }

    println("\n\nconfirm filter vertices")
    filteredVertices.collect.foreach(println(_))
    // (2,(Jiro,200))
    // (3,(Sabo,300))

    // 除了mapVertices之外，也可以使用VertexRDD的mapValues来修改顶点的属性
    val mappedVertices = vertices.mapValues((vid,attr) => attr._2 * attr._1.length)
    println("\n\nconfirm mapped vertices")
    mappedVertices.collect.foreach(println(_))
//    (1,400)
//    (3,1200)
//    (2,800)

    println("\n\nconfirm diffed vertices")
    // Remove vertices from this set that appear in the other set
    val diffedVertices:VertexRDD[(String,Long)] = vertices.diff(filteredVertices)

    println(diffedVertices.collect.foreach(println(_)))
//    ()



    // day07-01-vertices.csv
    // 1,Japan
    // 2,USA
    val verticesWithCountry = sc.textFile("hdfs://S7SA053:8020/stat/vertices.csv").map(line =>{
      val cols = line.split(",")
      (cols(0).toLong,cols(1).trim)
    })

    // 使用leftJoin来合并两个数据集中的属性，以left为主，如果verticesWithCountry存在则赋值，不存在，则赋值为"World"
    val leftJoinedVertices = vertices.leftJoin(verticesWithCountry){
      (vid,left,right) => (left._1,left._2,right.getOrElse("World"))
    }

    println("\n\nconfrim leftjoined vertices")
    leftJoinedVertices.collect.foreach(println(_))
    // (2,(Jiro,200,USA))
    // (1,(Taro,100,Japan))
    // (3,(Sabo,300,World))

    // 使用innerJoin来合并两个顶点属性，以verticesWithCountry为主
    val innerJoinedVertices = vertices.innerJoin(verticesWithCountry){
      (vid,left,right) => (left._1,left._2,right)
    }
    println("\n\nconfirm innerJoined vertices")
    innerJoinedVertices.collect.foreach(println(_))
//    (1,(Taro,100,Japan))
//    (2,(Jiro,200,USA))


    // day07-02-vertices.csv
    // 1,10
    // 2,20
    // 2,21
    val verticesWithNum:RDD[(VertexId,Long)] = sc.textFile("hdfs://S7SA053:8020/stat/vertices-02.csv").map(line =>{
      val cols = line.split(",")
      (cols(0).toLong, cols(1).toLong)
    })
    // 使用aggregateUsingIndex将verticesWithNum中相同Id中的属性相加, 对vertices没影响

    val auiVertices:VertexRDD[Long] = vertices.aggregateUsingIndex(verticesWithNum,_+_)
    println("\n\n confirm aggregateUsingIndexed vertices")
    auiVertices.collect.foreach(println(_))



    //~~~~~~~~~~~~~~~~~~
    //2.EdgeRDD
    //~~~~~~~~~~~~~~~~~~

    // 使用mapValues对edge中的属性进行修改
    val mappedEdges:EdgeRDD[Long] = edges.mapValues(edge => edge.attr._1 +1)
    println("\n\nconfirm mapped edges")
    mappedEdges.collect.foreach(println(_))
    // Edge(1,2,101)
    // Edge(2,3,201)
    // Edge(3,1,301)

    // The reverse operator returns a new graph with all the edge directions reversed
    val reversedRdges :EdgeRDD[(Long,java.util.Date)] =edges.reverse

    println("\n\nconfirm reversed edges")
    reversedRdges.collect.foreach(println(_))
    // Edge(2,1,(100,Mon Dec 01 00:00:00 EST 2014))
    // Edge(3,2,(200,Tue Dec 02 00:00:00 EST 2014))
    // Edge(1,3,(300,Wed Dec 03 00:00:00 EST 2014))



    // day08-edges.csv
    // 1,2,love
    // 2,3,hate
    // 3,1,even

    val e2:RDD[Edge[String]] =sc.textFile("hdfs://S7SA053:8020/stat/edges-02.csv").map(line =>{
      val cols = line.split(",")
      Edge(cols(0).toLong,cols(1).toLong,cols(2).trim)
    })

    val graph2:Graph[(String,Long),String] = Graph(v,e2)
    val edges2 = graph2.edges

    val innerJoinedEdge:EdgeRDD[(Long,java.util.Date,String)]=edges.innerJoin(edges2){
      (v1,v2,attr1,attr2)=> (attr1._1, attr1._2,attr2)
    }

    println("\n\nconfirm innerJoined edge")
    innerJoinedEdge.collect.foreach(println(_))
//    Edge(1,2,(100,Mon Dec 01 00:00:00 CST 2014,love))
//    Edge(2,3,(200,Tue Dec 02 00:00:00 CST 2014,hate))
//    Edge(3,1,(300,Wed Dec 03 00:00:00 CST 2014,even))



    // 1,2,love
    // 2,3,hate
    val e3:RDD[Edge[String]] =sc.textFile("hdfs://S7SA053:8020/stat/edges-03.csv").map(line =>{
      val cols = line.split(",")
      Edge(cols(0).toLong,cols(1).toLong,cols(2).trim)
    })
    val graph3:Graph[(String,Long),String] = Graph(v,e3)
    val edges3 = graph3.edges
    //
    val innerJoinedEdge2:EdgeRDD[(Long,java.util.Date,String)]=edges.innerJoin(edges3){
      (v1,v2,attr1,attr2)=> (attr1._1, attr1._2,attr2)
    }

    println("\n\nconfirm innerJoined edge")
    innerJoinedEdge2.collect.foreach(println(_))
//    Edge(1,2,(100,Mon Dec 01 00:00:00 CST 2014,love))
//    Edge(2,3,(200,Tue Dec 02 00:00:00 CST 2014,hate))

    sc.stop()

  }
}
