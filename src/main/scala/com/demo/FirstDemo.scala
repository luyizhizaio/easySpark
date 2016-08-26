package com.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/22.
 */
object FirstDemo {

  def main (args: Array[String]) {

    val file = "hdfs://S7SA053:8020/stat/temp.txt"
    val conf= new SparkConf().setAppName("first demo").setMaster("local[2]")//设置本地两个线程
    val sc = new SparkContext(conf)
    val data = sc.textFile(file,2).cache()

    val numas = data.filter(line => line.contains("b")).count()
    val numbs = data.filter(line => line.contains("b")).count()

    println("Line with a : %s , line with b :%s".format(numas,numbs))
  }

}
