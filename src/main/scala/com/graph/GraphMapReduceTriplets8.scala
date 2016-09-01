package com.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.util.GraphGenerators

/**
 * Created by lichangyue on 2016/9/1.
 */
object GraphMapReduceTriplets8 {



  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //随机生成一张图

    val graph = GraphGenerators.logNormalGraph(sc,numVertices = 100).mapVertices((id,_) => id.toDouble)
    //mkString 格式内容
    graph.vertices.take(10).mkString("\n").foreach(print)


    graph.edges.take(10).mkString("\n").foreach(print)

    //如果顶点id为年龄，计算所有比这个用户年龄大的用户的个数以及比这个用户年龄大的平均年龄
   /* val olderFollowes = graph.mapReduceTriplets(
    triplet => {//Map Function
      if(triplet.srcAttr > triplet.dstAttr){
        Iterator((triplet.dstId,(1, triplet.srcAttr)))
      }else{
        Iterator.empty
      }
    },
    (a, b) => (a._1 + b._1 ,a._2 + b._2)
    )*/
  }


}
