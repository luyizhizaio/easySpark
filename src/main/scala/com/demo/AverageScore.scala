package com.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 求每位同学的平均成绩
 * Created by lichangyue on 2016/8/23.
 */
object AverageScore {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("averageScore").setMaster("local")
    val sc = new SparkContext(conf)

    val math = sc.textFile("hdfs://S7SA053:8020/stat/math")
    val chinese = sc.textFile("hdfs://S7SA053:8020/stat/chinese")
    val english = sc.textFile("hdfs://S7SA053:8020/stat/english")


    val all = math.union(chinese).union(english)
    val res = all.filter(_.trim().length >0)
      .map(line => (line.split("\t")(0).trim(),line.split("\t")(1).trim().toInt))
      .groupByKey() //合并value到iterable[V]
      .map(x => {
        var num =0.0
        var sum =0
        for(i <- x._2){
          sum =sum +i
          num = num +1
        }
        val avg =sum/num
        val format = f"$avg%1.2f".toDouble
        (x._1,format) //返回值
      })
      .collect
      .foreach(x => println (x._1 +"\t"+ x._2))


  }

}
