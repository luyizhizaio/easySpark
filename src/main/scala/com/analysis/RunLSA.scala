package com.analysis

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/29.
 * 潜在语义分析
 */
object RunLSA {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sparkConf = new SparkContext(conf)


    //处理文本







  }
}


