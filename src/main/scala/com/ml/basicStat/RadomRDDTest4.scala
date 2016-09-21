package com.ml.basicStat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/21.
 */
object RadomRDDTest4 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)


    val randomNum = RandomRDDs.normalRDD(sc,10)
    println("normalRDD:")
    randomNum.foreach(println)
//    -0.4537726929742436
//    -0.491870023083511
//    1.2219006903663985
//    -0.19452221127411434
//    -0.6438891630514124
//    0.34999655901065296
//    -0.14065885112680288
//    1.2074431928507554
//    -0.504878366715275
//    0.9264369413274032

    println("uniformRDD:")
    RandomRDDs.uniformRDD(sc, 10).foreach(println)
    println("poissonRDD:")
    RandomRDDs.poissonRDD(sc, 5,10).foreach(println)
    println("exponentialRDD:")
    RandomRDDs.exponentialRDD(sc,7, 10).foreach(println)
    println("gammaRDD:")
    RandomRDDs.gammaRDD(sc, 3,3,10).foreach(println)

    sc.stop


//    uniformRDD:
//    0.6537054979198049
//    0.6761505119281546
//    0.1585859498696549
//    0.7566005263543939
//    0.551971267373723
//    0.667572816849627
//    0.24731905820816458
//    0.5870376435605084
//    0.03887075701704268
//    0.22990319033927875
//    poissonRDD:
//    4.0
//    6.0
//    4.0
//    6.0
//    7.0
//    9.0
//    2.0
//    6.0
//    7.0
//    7.0
//    exponentialRDD:
//    3.373821002107636
//    6.036829965236016
//    6.270145806255347
//    3.434740867374806
//    10.589785693760287
//    6.185824429551193
//    1.2270076754043988
//    6.800912446015353
//    0.7986970142270184
//    27.44527999188336
//    gammaRDD:
//    6.0323130732380985
//    6.193299320464891
//    3.3473082601272357
//    17.14372777269626
//    7.227113010028498
//    6.833633107546029
//    8.350287827557514
//    22.432137782488248
//    12.942233836579836
//    10.956198806276845
  }


}
