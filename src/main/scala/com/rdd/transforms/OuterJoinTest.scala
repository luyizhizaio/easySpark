package com.rdd.transforms

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/9/22.
 */
object OuterJoinTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)



    val rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
    println("leftOuterJoin")
    rdd1.leftOuterJoin(rdd2).collect.foreach(println(_))
//    (B,(2,None))
//    (A,(1,Some(a)))
//    (C,(3,Some(c)))

    println("rightOuterJoin")
    rdd1.rightOuterJoin(rdd2).collect.foreach(println(_))
//    (D,(None,d))
//    (A,(Some(1),a))
//    (C,(Some(3),c))


    println("subtractByKey")
    rdd1.subtractByKey(rdd2).collect.foreach(println(_))
//    (B,2)
  }

}
