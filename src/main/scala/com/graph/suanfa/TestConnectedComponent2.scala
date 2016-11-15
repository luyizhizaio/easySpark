package com.graph.suanfa

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/11/5.
 */
object TestConnectedComponent2 {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test1").setMaster("local")
    val sc = new SparkContext(conf)


    val dataSource=List("1 1","2 1","2 2","3 2",
      "4 3","5 3","5 4","6 4","6 5","7 5",
      "8 7","9 7","9 8","9 9","10 8","11 9")


    val rdd = sc.parallelize(dataSource).map(x =>{
      val data = x.split(" ")
      (data(0).toLong , data(1).toLong)
    })
    //(2 ,(1, 2, 3))
    val vertexRDD = rdd.groupBy(_._1).map(x =>{(x._1,x._2.unzip._2.min)})
    vertexRDD.foreach(println)











  }

}
