package com.initialdemo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/19.
 */
object NewMaxTemperature {


  def main (args: Array[String]) {

    val sc= new SparkContext(new SparkConf().setAppName("maxtemp").setMaster("local[2]"))

    val one = sc.textFile("hdfs://S7SA053:8020/stat/temp.txt")

    //map之前使用filter过滤非法数据
    val yearandtemp = one.filter(line => {   //匿名函数
      val quality = line.substring(50,51)
      var airTemperature = 0
      if(line.charAt(45)=='+'){
        airTemperature = line.substring(46,50).toInt

      } else {
        airTemperature = line.substring(45,50).toInt
      }
      airTemperature != 9999 && quality.matches("[01459]") //过滤返回结果
    }).map{
      line =>{
        val year = line.substring(15,19)
        var airTemperature = 0
        if(line.charAt(45) =='+'){
          airTemperature = line.substring(46,50).toInt
        }else{
          airTemperature = line.substring(45,50).toInt
        }
        (year,airTemperature) //map的返回结果
      }
    }

    val res = yearandtemp.reduceByKey((x,y) => if(x>y) x else y)//输出最大的key

    res.collect.foreach(x => println(("year :" +x._1 +",max:" + x._2))) //遍历数组,打印两个参数

  }
}
