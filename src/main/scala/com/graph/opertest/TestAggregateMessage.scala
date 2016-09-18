package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/18.
 */
object TestAggregateMessage {


  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf()
    val sc = new SparkContext("local","text",conf)
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph = GraphGenerators.logNormalGraph(sc,numVertices = 10)
      .mapVertices((id,_) => id.toDouble)

    graph.vertices.collect.foreach(println(_))

    graph.edges.collect.foreach(println(_))

    // Compute the number of older followers and their total age
    val olderFollowers:VertexRDD[(Int,Double)] = graph.aggregateMessages[(Int,Double)](
      triplet =>{// Map Function
        if(triplet.srcAttr > triplet.dstAttr){
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },

      (a,b) => (a._1 + b._1, a._2 + b._2)// Reduce Function, 对sendToDst 中的值做reduce操作
    )

    println("")
    olderFollowers.collect.foreach(println(_))
//    (4,(2,17.0))
//    (0,(4,18.0))
//    (1,(3,11.0))
//    (6,(3,24.0))
//    (3,(3,18.0))
//    (7,(2,17.0))
//    (8,(2,18.0))
//    (5,(2,16.0))
//    (2,(2,16.0))

    val avgAgeOfOlderFollowers = olderFollowers.mapValues((id,value) =>value match{
      case (count,totalAge) => totalAge/count
    })
    avgAgeOfOlderFollowers.collect().foreach(println(_))
//    (4,8.5)
//    (0,4.5)
//    (1,3.6666666666666665)
//    (6,8.0)
//    (3,6.0)
//    (7,8.5)
//    (8,9.0)
//    (5,8.0)
//    (2,8.0)
  }
}
