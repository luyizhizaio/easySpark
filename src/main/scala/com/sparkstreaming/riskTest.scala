package com.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph, Edge}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/10/10.
 */
object riskTest {

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

      //.filter{case (id,(n,ty)) => regex.findFirstMatchIn(id) != None}

    val regex="""^\d+$""".r

   /* val verteices = pointRDD.map(line =>{
      var points = line.split("\001")
      (points(0).toLong , (points(1),points(2).trim))
    })*/

    val verteices = pointRDD.map(line =>{
      var points = line.split("\001")
      (points(0), (points(1),points(2).trim))
    }).filter{case (id,(va,ty)) => regex.findFirstMatchIn(id) != None && id != null && !id.trim.equals("")}.map( points => {
      (points._1.toLong, points._2)
    })

    verteices.take(10).foreach(println(_))


    val edgeRDD = sc.textFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/edge_2016/")

    //5616103	191765607464	0.0	 time	login	11-19
    val edges = edgeRDD.map(line =>{
      var datas = line.split("\001")
      Edge(datas(0).toLong,datas(1).toLong,(datas(2),datas(3),datas(4),datas(5).trim))
    })

    /*val edges = edgeRDD.map(line =>{
      var datas = line.split("\001")
      (datas(0),datas(1),(datas(2),datas(3),datas(4),datas(5).trim))
    }).filter{case (sid, dstid,(v1,v2,v3,ty)) =>
      regex.findFirstMatchIn(sid) != None && sid != null && !sid.trim.equals("") && regex.findFirstMatchIn(dstid) != None && dstid != null && !dstid.trim.equals("")}.map(datas =>{
      Edge(datas._1.toLong,datas._2.toLong,datas._3)
    })*/



    val graph =Graph(verteices,edges).cache()


    //1.生成子图 ，边属性 ：账户-电话
   /* val userMobileGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-12")

    val mobileNum = userMobileGraph.outDegrees

    val mobileNumRDD  = mobileNum.map( line => {
      line._1 +","+ line._2
    })
    mobileNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/mobileNum2016")*/



    //--账户-邮箱 11-13
    /*val userMailGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-13")
    val mailNum = userMailGraph.outDegrees

    mailNum.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/mailNum2016_4")*/

    //--账户-姓名 11-14
    val userNameGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-14")
    val nameNum = userNameGraph.outDegrees


    val nameNumRDD  = nameNum.map( line => {
      line._1 +","+ line._2
    })

    nameNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/nameNum2016")

    //--账户-详细地址 11-15
    val userDetailAddressGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-15")
    val detailAddressNum = userDetailAddressGraph.outDegrees

    val detailAddressNumRDD  = detailAddressNum.map( line => {
      line._1 +","+ line._2
    })

    detailAddressNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/detailAddressNum2016")



    //--账户-标准地址 11-16
    val userStandardAddressGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-16")
    val standardAddressNum = userStandardAddressGraph.outDegrees


    val standardAddressNumRDD  = standardAddressNum.map( line => {
      line._1 +","+ line._2
    })

    standardAddressNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/standardAddressNum2016")



    //--账户-设备指纹 11-17
    val userUfpdGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-17")
    val ufpdNum = userUfpdGraph.outDegrees

    val ufpdNumRDD  = ufpdNum.map( line => {
      line._1 +","+ line._2
    })

    ufpdNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/ufpdNum2016")


    //--账户-ip地址 11-18
    val userIpGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-18")
    val ipNum = userIpGraph.outDegrees.filter{case (id,num) => id.toString.substring(0,1).equals("11")}

    val ipNumRDD  = ipNum.map( line => {
      line._1 +","+ line._2
    })

    ipNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/ipNum2016")


    //--账户-证件 11-19
    val userIdcardGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-19").cache()
    val idcardNum = userIdcardGraph.outDegrees.filter{case (id,num) => id.toString.substring(0,1).equals("11")}

    val idcardNumRDD  = idcardNum.map( line => {
      line._1 +","+ line._2
    })

    idcardNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/idcardNum2016")

    //--账户-sku 11-22
    val userSkuGraph = graph.subgraph(epred = (et) =>et.attr._4 == "11-22").cache()
    val skuNum = userSkuGraph.outDegrees.filter{case (id,num) => id.toString.substring(0,1).equals("11")}

    val skuNumRDD  = skuNum.map( line => {
      line._1 +","+ line._2
    })

    skuNumRDD.saveAsTextFile("hdfs://S1SA300:8020/user/hive/warehouse/relation.db/total/skuNum2016")

  }

}
