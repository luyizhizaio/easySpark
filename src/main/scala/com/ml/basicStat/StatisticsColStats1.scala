package com.ml.basicStat

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/20.
 */
object StatisticsColStats1 {


  def main(args: Array[String]) {


    val conf = new SparkConf()
    val sc = new SparkContext("local","test",conf)

//    1 2 3
//    4 5 6
    val vrdd = sc.textFile("file/data/mllib/input/basic/MatrixRow.txt") //读取文件
//    1
//    2
//    3
//    4
//    5
//    val vrdd = sc.textFile("file/data/mllib/input/basic/stats.txt")
    .map(_.split(" ")
    .map(_.toDouble))
    .map(line => Vectors.dense(line))

    vrdd.collect.foreach(println(_))

    val summary:MultivariateStatisticalSummary = Statistics.colStats(vrdd)

    println("每列max:"+summary.max)
    println("每列min:"+summary.min)
    println("每列mean:"+summary.mean)

    println("每列count:" + summary.count)
    println("每列非零元素的数量："+summary.numNonzeros)

    println("每列方差："+summary.variance)

    println("每列曼哈顿距离:"+summary.normL1) //计算曼哈顿距离:相加
    println("每列欧几里得距离:"+summary.normL2) //计算欧几里得距离：平方根



  }

}
