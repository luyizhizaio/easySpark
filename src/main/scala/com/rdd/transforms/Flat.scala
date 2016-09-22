package com.rdd.transforms

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/8/25.
 */
object Flat {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("flat").setMaster("local")

    val sc = new SparkContext(conf)

    val map = sc.textFile("hdfs://S7SA053:8020/stat/map",2)

    val flatrdd = map.flatMap(line => line.split(","))

    println("分区长度"+flatrdd.partitions.length)
  }

}
