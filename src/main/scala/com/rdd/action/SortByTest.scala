package com.rdd.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/22.
 */
object SortByTest {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(Seq(3,6,7,1,2,0),2)
    //默认升序
    val result1= rdd1.sortBy(x => x).collect.mkString(",")
    println(result1)
//    0,1,2,3,6,7

    val result2= rdd1.sortBy(x => x,false).collect.mkString(",")
    println(result2)
//    7,6,3,2,1,0

    //按照K进行升序
    var rdd2 = sc.makeRDD(Array(("A",2),("A",6),("B",6),("B",3),("B",7)))
    val result3 = rdd2.sortBy(x => x).collect.mkString(",")
    println(result3)
//    (A,2),(A,6),(B,3),(B,6),(B,7)

    //按照V进行降序排序
    val result4 = rdd2.sortBy(x => x._2,false).collect.mkString(",")
    println(result4)
//    (B,7),(A,6),(B,6),(B,3),(A,2)


  }


}
