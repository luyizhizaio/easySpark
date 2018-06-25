package com.initialdemo

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 *
 * Created by lichangyue on 2016/8/23.
 */
object DataSort {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("dataSort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sort1 = sc.textFile("hdfs://S7SA053:8020/stat/sort1",3)
    val sort2 = sc.textFile("hdfs://S7SA053:8020/stat/sort2",5)

    val sort = sort1.union(sort2)

    var  idx = 0 //变量
    sort.filter(_.trim().length >0).map(num => (num.trim.toInt,"")).
      partitionBy(new HashPartitioner(1)).
      sortByKey().map(t =>{
      idx  += 1
      (idx, t._1)  //map的返回值
    }).collect.foreach(x=> println(x._1 +"\t"+x._2))
  }
}
