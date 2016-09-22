package com.rdd.transforms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/22.
 */
object JoinTest {

//join相当于SQL中的内关联join，只返回两个RDD根据K可以关联上的结果，
// join只能用于两个RDD之间的关联，如果要多个RDD关联，多关联几次即可。
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)


    rdd1.join(rdd2).collect.foreach(println(_))
//  (A,(1,a))
//  (C,(3,c))

  }


}
