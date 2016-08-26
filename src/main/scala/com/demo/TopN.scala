package com.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/23.
 */
object TopN {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("topN").setMaster("local")
    val sc =new SparkContext(conf)

    val ardd = sc.textFile("hdfs://S7SA053:8020/stat/a.txt",3)
    val brdd = sc.textFile("hdfs://S7SA053:8020/stat/b.txt",5)

    val a = ardd.filter(line => line.trim().length> 0 && line.split(",").length ==4)
    val b = brdd.filter(line => line.trim().length> 0 && line.split(",").length ==4)
    var idx = 0
    val  rdd = a.union(b)
    val res = rdd.map(_.split(",")(2))  //返回第三个元素
      .map(x => (x.toInt ,""))
      .sortByKey(false) //降序
      .map(x => x._1)
      .take(5)
      .foreach(x => {
        idx = idx + 1
        println( idx +","+ x)
    })
  }
}
