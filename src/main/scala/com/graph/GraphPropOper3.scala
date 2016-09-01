package com.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 图操作的属性操作，对顶点、边 、triplets 进行map操作
 * Created by lichangyue on 2016/8/31.
 */
object GraphPropOper3 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    /**
     *  mapVertices 转换顶点的属性
     *
     */

    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //指定分区数量
    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt",numEdgePartitions=4)

    //查看顶点元素的属性都为1，(185012,1)
    graph.vertices.take(10).foreach(println)

    /***mapVertices：将顶点的属性值改为2，*/
    val tmp = graph.mapVertices((id,attr) => attr.toInt * 2)
    tmp.vertices.take(10).foreach(println)

    //查看边的属性也为1， Edge(0,11342,1)
    graph.edges.take(10).foreach(println)

    //mapEdges：修改边的属性为2
    val tmp2 = graph.mapEdges(e => e.attr.toInt * 2) //attr属性来自Edge类

    tmp2.edges.take(10).foreach(println)

    //mapTriplets： 把元素的edge的属性值设置为源顶点属性值2倍加上目标顶点的3倍
    val tmp3 = graph.mapTriplets(et => et.srcAttr.toInt * 2 + et.dstAttr *3 )
    //((1,1),(53051,1),5)
    tmp3.triplets.take(10).foreach(println)
    //Edge(0,11342,5)
    tmp3.edges.take(10).foreach(println)

  }

}
