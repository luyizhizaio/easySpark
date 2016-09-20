package com.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/1.
 */
object GraphTriangleCount12 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //第三个参数canonicalOrientation=true表示只有源顶点大于目标顶点才算一个
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/web-Google.txt", true)


    //计算通过每一个顶点的三角数量
    val c = graph.triangleCount().vertices

    c.take(10).foreach(println)

  }



  }
