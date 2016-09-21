package com.ml.basicStat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 分层抽样
 * Created by lichangyue on 2016/9/21.
 */
object StratifiedSamplingTest2 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

//    aa
//    bb
//    cc
//    dd
//    ee
//    aaa
//    bbb
//    ccc
//    ddd
//    eee
    val data:RDD[(Int,String)] = sc.textFile("file/data/mllib/input/basic/StratifiedSampling.txt") //读取数
      .map(line => {
      if (line.trim.length ==3){
        (line, 1)
      }else {
        (line, 2)
      }
    }).map(line => (line._2,line._1))

    println("\n\nconfirm data")
    data.foreach(println(_))
//    (2,aa)
//    (1,bbb)
//    (2,bb)
//    (1,ccc)
//    (2,cc)
//    (1,ddd)
//    (2,dd)
//    (1,eee)
//    (2,ee)
//    (1,aaa)

    //设定抽样格式
    val fractions:Map[Int,Double] = (List((1,0.2),(2,0.8))).toMap
    //计算抽样样本
    val approxSample =data.sampleByKey(withReplacement= false,fractions,0)

    println("sampleByKey:")
    approxSample.foreach(println(_))
//    (1,bbb)
//    (2,bb)
//    (1,ddd)
//    (2,cc)
//    (2,ee)


    println("Second:")
    //http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#sampleByKey
    val randRDD = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    val sampleMap = List((7, 0.4), (6, 0.8)).toMap
    val sample2 = randRDD.sampleByKey(false, sampleMap, 42).collect
    sample2.foreach(println)
//    (6,mouse)
//    (7,cup)
//    (6,book)
//    (6,screen)

    println("Third:")
    //http://bbs.csdn.net/topics/390953396
    val a = sc.parallelize(1 to 20, 3)
    val b = a.sample(true, 0.8, 0)
    val c = a.sample(false, 0.8, 0)
    println("RDD a : " + a.collect().mkString(" , "))
    println("RDD b : " + b.collect().mkString(" , "))
    println("RDD c : " + c.collect().mkString(" , "))
    sc.stop
//    RDD a : 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20
//    RDD b : 2 , 4 , 5 , 6 , 10 , 14 , 19 , 20
//    RDD c : 1 , 2 , 4 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 19

  }




}
