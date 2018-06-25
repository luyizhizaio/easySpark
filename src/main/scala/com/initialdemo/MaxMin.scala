package com.initialdemo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/23.
 */
object MaxMin {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("maxmin").setMaster("local")
    val sc = new SparkContext(conf)

    val file1 = sc.textFile("hdfs://S7SA053:8020/stat/sort1",2)
    val file2 = sc.textFile("hdfs://S7SA053:8020/stat/sort2",3)
    val file = file1.union(file2)
    val res = file.filter(_.trim.length > 0)
      .map(line => ("key", line.trim.toInt))
      .groupByKey()
      .map(x => {
        var min  = Integer.MAX_VALUE
        var max = Integer.MIN_VALUE
        for(num <-x._2){
          if(num > max){
            max = num
          }
          if (num < min){
            min = num
          }
        }
      (max, min) //返回值
    }).collect.foreach(x=> {
      println("max:"+x._1)
      println("min:"+x._2)
    })
  }
}
