package com.rdd.checkpoint

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kyrie on 2018/7/2.
 */
object CheckPointTest {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "E:\\openware\\hadoop-2.2.0")

    val conf  = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("data/checkpoint") //设置checkpoint目录

    val data = Array[(Int,Char)]((1,'a'),(2,'b'),
    (3, 'c'), (4, 'd'),
		    						 (5, 'e'), (3, 'f'),
		    						 (2, 'g'), (1, 'h')
    )

    val pairs = sc.parallelize(data,3)
    pairs.checkpoint()  //checkpoint
    pairs.count()

    val result = pairs.groupByKey(2)
    result.foreach(println(_))
    result.foreachWith(i=>i)((x,i) => println(s"[PartitionIndex: $i ] $x"))

    println(result.toDebugString)



  }

}
