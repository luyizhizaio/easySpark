package test.gome.architect

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lichangyue on 2016/10/10.
  */
object riskTest2 {

   /**
    * 目标是账户关联de电话
    */

   def main(args: Array[String]) {
     Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

     val conf = new SparkConf().setAppName("riskTest").setMaster("spark://fk01:7077")
       .set("spark.network.timeout","1000s").set("spark.executor.heartbeatInterval","1000s")

     val sc = new SparkContext(conf)

     val pointRDD = sc.textFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/point_2016/")
     //1000066	ld8hnk7i0	11

     val verteices = pointRDD.map(line =>{
       var points = line.split("\001")
       (points(0).toLong , (points(1),points(2).trim))
     })

     val edgeRDD = sc.textFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/edge_2016/")

     //5616103	191765607464	0.0	 time	login	11-19
     val edges = edgeRDD.map(line =>{
       var datas = line.split("\001")
       Edge(datas(0).toLong,datas(1).toLong,(datas(2),datas(3),datas(4),datas(5).trim))
     })

     val graph =Graph(verteices,edges).cache()



     //--账户-邮箱 11-13
     val userMailGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-13")
     val mailNum = userMailGraph.outDegrees

     val mailNumRDD  = mailNum.map( line => {
       line._1 +","+ line._2
     })

     mailNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/mailNum2016")

     //--账户-ip地址 11-18
     val userIpGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-18")
     val ipNum = userIpGraph.outDegrees

     val ipNumRDD  = ipNum.map( line => {
       line._1 +","+ line._2
     })

     ipNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/ipNum2016")


     //--账户-证件 11-19
     val userIdcardGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-19")
     val idcardNum = userIdcardGraph.outDegrees

     val idcardNumRDD  = idcardNum.map( line => {
       line._1 +","+ line._2
     })

     idcardNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/idcardNum2016")

     //--账户-sku 11-22
     val userSkuGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-22")
     val skuNum = userSkuGraph.outDegrees

     val skuNumRDD  = skuNum.map( line => {
       line._1 +","+ line._2
     })

     skuNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/skuNum2016")

     sc.stop()
   }
 }
