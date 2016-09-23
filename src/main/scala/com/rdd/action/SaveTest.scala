package com.rdd.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/22.
 */
object SaveTest {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(1 to 10,2)

    rdd1.saveAsTextFile("hdfs://S7SA053:8020/tmp/saveas")



  }

}
