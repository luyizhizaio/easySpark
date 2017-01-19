package com

import com.streaming.utils.RiskDateUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/10/31.
 */
object SparkTest {


  def main(args: Array[String]) {

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

    /*val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
      .set("spark.network.timeout", "99999s").set("spark.executor.heartbeatInterval", "99999s")

    val sc = new SparkContext(conf)

    val x = sc.parallelize(List(Set("a"), Set("b"), Set("c", "d")))

    x.flatMap(y=>y).foreach(a => println(a))*/



      val conf = new SparkConf().setAppName("test").setMaster("local")

      val sc = new  SparkContext(conf)

     /* val rdd = sc.parallelize(List(1,2,3))

      val num = rdd.map(x =>x +1).reduce(_+_)

      println(num)*/

    val rdd = sc.textFile("file/temp/time.txt")
    rdd.map(line =>{
      //20-10月-16 03.25.15.000000 下午
      var arr = line.split(" ")
      var datearr = arr(0).split("-") //20-10月-16
      var date = "20" + datearr(2) +"-" + datearr(1).substring(0,datearr(1).length-1) +"-"+ datearr(0)
      var timearr = arr(1).split("\\.")
      var ampm =  arr(2)
      var hour = timearr(0)
      if("下午".equals(ampm)){
        hour = (timearr(0).toInt + 12).toString
      }
      var time = hour + ":" + timearr(1) + ":" + timearr(2)
      date +" "+ time
//      val date = "2016-11-09 00:00:00"


    }).foreach(println)

  }






}
