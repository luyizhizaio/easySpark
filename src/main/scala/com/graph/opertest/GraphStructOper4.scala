package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 图操作的结构操作 ：主要方法有：
 * reverse：颠倒边的顶点顺序
 * subgraph：返回满足条件的子图，参数1是边的条件，参数2是顶点的条件
 * mask：
 * groupEdges：
 *
 * Created by lichangyue on 2016/8/31.
 */
object GraphStructOper4 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    /**
     * mapVertices 转换顶点的属性
     *
     */

    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //指定分区数量
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/web-Google.txt", numEdgePartitions = 4)


    //查看顶点数
    val vcount = graph.vertices.count()
    println("vcount:" +vcount)

    //查看边数
    val eCount = graph.edges.count()
    println("eCount:" +eCount)

    //1.subgraph :使用边做限制
    val subgraph = graph.subgraph(epred = e => e.srcId > e.dstId)

    subgraph.edges.take(10).foreach(println)
    //查看顶点数，顶点数与原来的相同
    val vcount2 = subgraph.vertices.count()
    println("vcount2:" +vcount2) //875713
    //查看边数
    val eCount2 = subgraph.edges.count()
    println("eCount2:" +eCount2) //2420548

    //2.subgraph：加入顶点过滤
    val subgraph2 = graph.subgraph(epred = e => e.srcId > e.dstId,
      vpred = (id,_) => id > 500000)

    val vcount3 = subgraph2.vertices.count()
    println("vcount3:" +vcount3) //400340

    val subgraph3 = graph.subgraph(epred = e => e.srcId > e.dstId,
      vpred = (id,_) => id > 10000000)

    //
    val vcount4 = subgraph3.vertices.count()
    println("vcount4:" +vcount4) //0





  }

}
