package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/31.
文件的数据结构
# Nodes: 875713 Edges: 5105039
# FromNodeId	ToNodeId
0	11342
0	824020
0	867923
0	891835
11342	0
11342	27469
 */
object GraphFromFile2 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

//    val conf = new SparkConf().setAppName("graph2").setMaster("spark://10.58.22.219:7077")
    val conf = new SparkConf().setAppName("graph2").setMaster("local[4]")
    val sc = new SparkContext(conf)
//    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt")

    //指定分区数量
    val graph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/web-Google.txt",numEdgePartitions=4)

    //查看顶点数量
    val vcount = graph.vertices.count()
    println("vcount:" +vcount)
    //查看边的数量
    val ecount = graph.edges.count()
    println("ecount:" + ecount)

  }

}
