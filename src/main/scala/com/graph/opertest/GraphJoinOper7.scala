package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 *
 *
 * Created by lichangyue on 2016/9/1.
 */
object GraphJoinOper7 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    //    val conf = new SparkConf().setAppName("graph2").setMaster("spark://10.58.22.219:7077")
    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //指定分区数量
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/web-Google.txt", numEdgePartitions = 4)

    //所有顶点的属性设置为0
    val rawGraph = graph.mapVertices((id,attr) => 0)
    rawGraph.vertices.take(10).foreach(println)


    //
    val outDeg = rawGraph.outDegrees
    //1.joinVertices
    val tmp = rawGraph.joinVertices(outDeg)((_,_,optDeg) => optDeg)

    tmp.vertices.take(10).foreach(println)

    println("--------------------")
    //outJoinVertices用法
    val tmp2 = rawGraph.outerJoinVertices(outDeg)((_,_,optDeg) => optDeg.getOrElse(0))

    tmp2.vertices.take(20).foreach(println)


  }


}
