package com.graph.suanfa

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{GraphLoader, Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/1.
 * 标签传播
 */
object LPAAlgorithm {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val  conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA054:8020/stat/web-Google.txt", numEdgePartitions = 4)

    //参数：图，迭代次数
    val lpaGraph = LabelPropagation.run(graph, 20)

    //lpaGraph.triplets.take(10).foreach(println(_))

    lpaGraph.vertices.sortBy(x =>x._2).saveAsTextFile("hdfs://S7SA054:8020/stat/label-web-Google")

    sc.stop()


  }



}
