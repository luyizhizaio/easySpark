package com.rdd.transforms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/21.
 */
object CogroupTest {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    //##参数为1个RDD的例子

    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)

    var rdd3 = rdd1.cogroup(rdd2)

    println(rdd3)
//    MapPartitionsRDD[3] at cogroup at CogroupTest.scala:21

    println("size:"+rdd3.partitions.size)
//    size:2

    rdd3.collect.foreach(println)
//    (B,(CompactBuffer(2),CompactBuffer()))
//    (D,(CompactBuffer(),CompactBuffer(d)))
//    (A,(CompactBuffer(1),CompactBuffer(a)))
//    (C,(CompactBuffer(3),CompactBuffer(c)))

    //指定分区
    val rdd4 = rdd1.cogroup(rdd2,3)


    println("size:"+rdd4.partitions.size)
//    size:3
    rdd4.collect.foreach(println)
//    (B,(CompactBuffer(2),CompactBuffer()))
//    (C,(CompactBuffer(3),CompactBuffer(c)))
//    (A,(CompactBuffer(1),CompactBuffer(a)))
//    (D,(CompactBuffer(),CompactBuffer(d)))




  //~~~~~~~~~~~~~~~~~~~~~~~
  //参数为2个RDD的例子
  //~~~~~~~~~~~~~~~~~~~~~~~









  }




}
