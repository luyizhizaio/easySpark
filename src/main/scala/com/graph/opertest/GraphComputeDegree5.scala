package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexId, GraphLoader, Graph}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 度计算，degree是GraphOps的成员
 * Created by lichangyue on 2016/8/31.
 */
object GraphComputeDegree5 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("degreeTest").setMaster("local")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //查看indegrees
    val tmp = graph.inDegrees
    //(113843,1)
    tmp.take(10).foreach(println)
    println("-------------")
    val outTmp = graph.outDegrees
    outTmp.take(10).foreach(println)
    println("-------------")
    val degrees = graph.degrees
    degrees.take(10).foreach(println)

    //找出最大的degrees 的顶点 a表示(顶点id,度)
    val maxdegree = graph.degrees.reduce{
      (a,b) => if(a._2 >b._2) a  else b
    }
    //537039,degree:6353
    println(maxdegree._1 + ",degree:" + maxdegree._2)

    //最大的indegree
    val maxindegree = graph.inDegrees.reduce{
      (a,b) => if(a._2 >b._2) a  else b
    }
    //537039,indegree:6326 . indegree大说明页面很好
    println(maxindegree._1 + ",indegree:" + maxindegree._2)

    //最大 outDegree

    val maxoutdegree = graph.outDegrees.reduce{
      (a,b) => if(a._2 >b._2) a  else b
    }
    //506742,outdegree:456 ，  outdegree大一般为导航网站
    println(maxoutdegree._1 + ",outdegree:" + maxoutdegree._2)


  }


  def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int) = if(a._2 > b._2) a else b

}
