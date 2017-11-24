package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *主要方法：
 * collectNeighborIds： 收集相邻的顶点id
 * collectNeighbprs：收集相邻的顶点属性
 * Created by lichangyue on 2016/9/1.
 */
object GraphCollectNeighbors6 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("degreeTest").setMaster("local")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/web-Google.txt")


    graph.collectNeighborIds(EdgeDirection.In).take(10).foreach(println)

  }


  }
