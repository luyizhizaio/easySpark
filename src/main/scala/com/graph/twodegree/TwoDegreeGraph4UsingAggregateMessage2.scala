package com.graph.twodegree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求出二度节点的数量
  * Created by lichangyue on 2016/11/15.
  */
object TwoDegreeGraph4UsingAggregateMessage2 {




   def main(args: Array[String]) {
     Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

     val conf = new SparkConf().setAppName("riskTest").setMaster("local")
       .set("spark.network.timeout", "1000s").set("spark.executor.heartbeatInterval", "1000s")
     val sc = new SparkContext(conf)

     val edge = List(//边的信息
       (111, 122), (111, 173), (112, 123), (111, 124), (113, 175), (113, 176),
       (114, 175), (115, 126), (117, 178), (117, 179), (118, 179))


     //构建边的rdd
     val edgeRdd = sc.parallelize(edge).map(x => {
       Edge(x._1.toLong, x._2.toLong, None)
     })
     //通过边构建图
     val graph = Graph.fromEdges(edgeRdd, 0)

     /**
      * 步骤一：找邻居
      * 1. 每个顶点，将自己的id，发送给自己所有的邻居
      * 2. 每个顶点，将收到的所有邻居id，合并为一个List
      * 3. 对新List进行排序，并和原来的图进行关联，附到顶点之上
      *
      */


     val idsVerts = graph.aggregateMessages[List[Long]](
       triplet =>{// Map Function
           // Send message to destination vertex containing counter and age
           triplet.sendToSrc(List(triplet.dstId))
           triplet.sendToDst(List(triplet.srcId))
       },

       (a,b) => (a.++(b).sorted)// Reduce Function, 对sendToDst 中的值做reduce操作

     )

     idsVerts.collect().foreach(println(_))
 //    (4,List(3, 5))
 //    (1,List(2, 3))
 //    (6,List(3, 5))
 //    (3,List(1, 2, 4, 5, 6))
 //    (7,List(8, 9))
 //    (9,List(7, 8))
 //    (8,List(7, 9))
 //    (5,List(3, 4, 6))
 //    (2,List(1, 3))

     //合并到原图上

     val graph2 = graph.mapVertices((id,attr) => List[Long]()).joinVertices(idsVerts)((vid,attr,user) => user)

    /* val graph2 = graph.mapVertices((id,attr) => "").joinVertices(users)(
       (vid,attr,user) =>user)*/
     println("after joining vertices: ")
     graph2.vertices.foreach(println(_))
 //    (4,List(3, 5))
 //    (1,List(2, 3))
 //    (6,List(3, 5))
 //    (3,List(1, 2, 4, 5, 6))
 //    (7,List(8, 9))
 //    (9,List(7, 8))
 //    (8,List(7, 9))
 //    (5,List(3, 4, 6))
 //    (2,List(1, 3))





     /**
      * 步骤二：1. 遍历所有的Triplet，对2个好友的有序好友List进行扫描匹配，数出共同好友数，并将其更新到edge之上
      */

     //id,num
     val rdd = graph2.triplets.map(triplet =>{
       var dstType = triplet.dstId.toString.substring(0,2)
       if(dstType =="17"){
         var mobileNum = 0
         triplet.srcAttr.foreach(x=>{
           var srcType = x.toString.substring(0,2)
           if(srcType == "12") mobileNum += 1
         })
         (triplet.dstId.toLong ,mobileNum)
       }else{
         (triplet.dstId.toLong ,0)
       }
     }).filter{case(id,num) =>  id.toString.substring(0,2)=="17"}


     rdd.foreach(println)
   }
 }
