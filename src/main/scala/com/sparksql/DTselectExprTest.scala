package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by lichangyue on 2016/11/29.
 */
object DTselectExprTest {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //    {"name":"Michael" ,"salary":50}
    //    {"name":"Andy", "age":30,"salary":100}
    //    {"name":"Flank", "age":19}
    val df = sqlContext.read.json("file/data/sql/person")

    val r = new Random()

    df.selectExpr("name","age", "concat(name ,"+r.nextInt()+") as name2").show()
/*
+-------+----+------------+
|   name| age|       name2|
+-------+----+------------+
|Michael|null|Michael-fuck|
|   Andy|  30|   Andy-fuck|
|  Flank|  19|  Flank-fuck|
+-------+----+------------+
 */



  }

}
