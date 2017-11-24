package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 使用pregel compute shortest paths 最短路劲计算
 * Created by lichangyue on 2016/9/1.
 */
object GraphPregel9 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("pregel").setMaster("local")
    val sc = new SparkContext(conf)

    var graph = GraphLoader.edgeListFile(sc,"file/data/graph/web-Google.txt")

    val sourceId = 0

    graph.vertices.take(10).foreach(println(_))
    //修改顶点属性值
    val g= graph.mapVertices((id,_) => if(id ==sourceId)0.0 else Double.PositiveInfinity)

    g.vertices.take(10).foreach(println(_))

    val sssp=g.pregel(Double.PositiveInfinity)(
       (id,dist,newDist) => math.min(dist,newDist),
       triplet=>{
         if(triplet.srcAttr + triplet.attr < triplet.dstAttr){

           Iterator((triplet.dstId,triplet.srcAttr + triplet.attr))
         }else{
           Iterator.empty
         }
       },
      (a,b) => math.min(a,b) //返回最小的一个
    )

    sssp.vertices.take(10).mkString("\n").foreach(print)

  }

}
