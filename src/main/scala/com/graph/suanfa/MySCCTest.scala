package com.graph.suanfa

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/21.
 */
object MySCCTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

//    1 2
//    2 3
//    3 1
//    4 5
//    5 6
//    6 7
    val graph = GraphLoader.edgeListFile(sc,"file/data/graph/scc.txt").cache()


    val g = MyStronglyConnectedComponent.run(graph,4)

    println("finally：")
    g.vertices.foreach(println)


  }
}
