package com.rdd.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/22.
 */
object ActionTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
    rdd1.top(2).foreach(println(_))
//    12
//    10
    //指定排序规则
    implicit val myOrd = implicitly[Ordering[Int]].reverse
    rdd1.top(2).foreach(println(_))
//    2
//    3


    rdd1.takeOrdered(2).foreach(println(_))
//    12
//    10

  }

}
