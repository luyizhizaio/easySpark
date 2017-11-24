package com.graph.opertest

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * graphOps provide method of pageRank.
 *
 * Created by lichangyue on 2016/9/1.
 */
object GraphPageRank11 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //指定分区数量
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/web-Google.txt", numEdgePartitions = 4)

    //传入的值越小越准确
    graph.pageRank(0.01).vertices.take(10).foreach(println)

  }

}
