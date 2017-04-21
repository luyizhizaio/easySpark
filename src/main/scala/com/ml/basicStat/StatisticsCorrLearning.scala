package com.ml.basicStat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/28.
 */
object StatisticsCorrLearning {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

//    1 2 3 4 5
//    6 7 8 9 10
    val rddX = sc.textFile("file/data/mllib/input/basic/StatisticsCorrLearningx.txt")
        .flatMap(_.split(" "))
        .map(_.toDouble)

//    1 2 3 4 5
//    6 7 8 9 10
    val rddY = sc.textFile("file/data/mllib/input/basic/StatisticsCorrLearningy.txt") //读取数据
      .flatMap(_.split(' ') //进行分割
      .map(_.toDouble)) //转化为Double类型

    println("rddx:")
    rddX.foreach(each => print(each + " "))
//    1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0

    println("\nrddy:")
    rddY.foreach(each => print(each + " "))
//    1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0


    //计算不同数据之间的相关系数:皮尔逊
    var correlationPearson:Double = Statistics.corr(rddX,rddY)
    println("\n\ncorrelationPearson:" + correlationPearson)
    //correlationPearson:1.0


    var correlationSpearman: Double = Statistics.corr(rddX, rddY, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman：" + correlationSpearman) //打印结果
//    correlationSpearman：1.0000000000000009



    ////////////////////////////////////////////////////////////
    println("\nsecond")
    var arr1 = (1 to 10 ).toArray.map(_.toDouble)
    var arr2 = (1 to 10 ).toArray.map(_.toDouble)

    var rdd1 = sc.parallelize(arr1)
    var rdd2 = sc.parallelize(arr2)

    println("rdd1:")
    rdd1.foreach(each => print(each + " "))
    println("\nrdd2:")
    rdd2.foreach(each => print(each + " "))

    correlationPearson = Statistics.corr(rdd1, rdd2) //计算不同数据之间的相关系数:皮尔逊
    println("\ncorrelationPearson2：" + correlationPearson) //打印结果
    //    correlationPearson：1.0
    correlationSpearman = Statistics.corr(rdd1, rdd2, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman2：" + correlationSpearman) //打印结果


//    correlationSpearman：1.0000000000000009


    println("\nThird:")
    val arr3 = (1 to 5).toArray.map(_.toDouble)
    val arr4 = (2 to 10 by 2).toArray.map(_.toDouble)
    val rdd3 = sc.parallelize(arr3)
    val rdd4 = sc.parallelize(arr4)
    println("rdd3:")
    rdd3.foreach(each => print(each + " "))
    println("\nrdd4:")
    rdd4.foreach(each => print(each + " "))

    val correlationPearson3 = Statistics.corr(rdd3, rdd4) //计算不同数据之间的相关系数:皮尔逊
    println("\ncorrelationPearson3：" + correlationPearson3) //打印结果

    val correlationSpearman3 = Statistics.corr(rdd3, rdd4, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman3：" + correlationSpearman3) //打印结果
//    Third:
//      rdd3:
//    1.0 2.0 3.0 4.0 5.0
//    rdd4:
//    2.0 4.0 6.0 8.0 10.0
//    correlationPearson3：0.9999999999999998
//    correlationSpearman3：0.9999999999999998

    println("\nFourth:")
    val rdd5 = sc.parallelize(Array(5.0, 3.0, 2.5))
    val rdd6 = sc.parallelize(Array(4.0, 3.0, 2.0))
    println("rdd5:")
    rdd5.foreach(each => print(each + " "))
    println("\nrdd6:")
    rdd6.foreach(each => print(each + " "))
    val correlationPearson5 = Statistics.corr(rdd5, rdd6) //计算不同数据之间的相关系数:皮尔逊
    println("\ncorrelationPearson5：" + correlationPearson5) //打印结果
    //与链接和书计算结果一致：http://blog.csdn.net/dc_726/article/details/40017997 书：ref
    val correlationSpearman5 = Statistics.corr(rdd5, rdd6, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman5：" + correlationSpearman5) //打印结果
//    Fourth:
//      rdd5:
//    5.0 3.0 2.5
//    rdd6:
//    4.0 3.0 2.0
//    correlationPearson5：0.944911182523068
//    correlationSpearman5：1.0


    println("\nFifth:")
    val rdd7 = sc.parallelize(Array(170.0, 150.0, 210.0,180.0,160.0))
    val rdd8 = sc.parallelize(Array(180.0, 165.0, 190.0,168.0,172.0))
    println("rdd:")
    rdd7.foreach(each => print(each + " "))
    println("\nrdd:")
    rdd8.foreach(each => print(each + " "))
    val correlationPearson7 = Statistics.corr(rdd7, rdd8) //计算不同数据之间的相关系数:皮尔逊
    println("\ncorrelationPearson：" + correlationPearson7) //打印结果
    //与链接和书计算结果一致：http://baike.baidu.com/link?url=s9aiihE4mMpF2sZLqR33JKz3FNk8R8IWWh9-ZgNFO4aZB5ez9mnADNQZQSApniWXUJGwhr-Ar9mjWEFVwncQlq书：ref

    val correlationSpearman7 = Statistics.corr(rdd7, rdd8, "spearman") //使用斯皮尔曼计算不同数据之间的相关系数
    println("correlationSpearman：" + correlationSpearman7) //打印结果

//    Fifth:
//      rdd:
//    170.0 150.0 210.0 180.0 160.0
//    rdd:
//    180.0 165.0 190.0 168.0 172.0
//    correlationPearson：0.8171759569273293
//    correlationSpearman：0.6999999999999998


  }

}
