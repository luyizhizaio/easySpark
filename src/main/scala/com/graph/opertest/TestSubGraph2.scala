package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/12/14.
 */
object TestSubGraph2 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
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


    var arr = Array(1,2,3)

    var sub = graphA.subgraph(vpred = (id,attr) => !arr.contains(attr))

    sub.triplets.foreach(println)

  }

}
