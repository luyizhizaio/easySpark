package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/30.
 */
object DataFrameTest4 {

  case class People(id:Int,name:String,age:Int)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("test4").setMaster("local")
    val sc =new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("")

    val peopleRDD = people.map(x =>{
      x.split(",")}).map(data => {
      People(data(0).toInt,data(1).trim,data(3).toInt)
    })

    //隐式转换
    import sqlContext.implicits._
    val df = peopleRDD.toDF()
    df.registerTempTable("people")

    df.show()
    df.printSchema()



  }

}
