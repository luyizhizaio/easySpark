package com

import com.streaming.utils.RiskDateUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/10/31.
 */
object SparkTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

   /* val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)



    var abc = List("aaa,bbb,ccc")

    var fuck = abc.flatMap(line  => line.split(","))

    //println(fuck.)

    fuck.foreach(println)*/

   /* var x= "2016-10-21T10:27:13+08:00"
    var dateArr = x.split("T")
    var hours = dateArr(1).split("\\+")

    var creationDate = dateArr(0) +" "+hours(0)

    println(creationDate)

    var ctdate =RiskDateUtil.formatDate(creationDate)

    println(ctdate)*/

    /*var map = Map(1111->2,1123->3,23112->5)

    println(map.toSeq)*/


   /* var set = Set[(Long,Long,String)]()

    println(set.size)*/

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
      .set("spark.network.timeout", "99999s").set("spark.executor.heartbeatInterval", "99999s")

    val sc = new SparkContext(conf)

    val x = sc.parallelize(List(Set("a"), Set("b"), Set("c", "d")))

    x.flatMap(y=>y).foreach(a => println(a))

  }






}
