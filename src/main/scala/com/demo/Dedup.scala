package com.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 数据去重
 * Created by lichangyue on 2016/8/23.
 */
object Dedup {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Dedup").setMaster("local")

    val sc = new SparkContext(conf)


    val file1RDD = sc.textFile("hdfs://S7SA053:8020/stat/file1.txt")
    val file2RDD = sc.textFile("hdfs://S7SA053:8020/stat/file2.txt")

    val file= file1RDD.union(file2RDD)

    file.filter(_.trim().length() >0).
      map(line => (line.trim() ,"")).
      groupByKey().  //分组去重
      sortByKey().   //排序
      keys.collect().foreach(println _)

  }

}
