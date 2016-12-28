package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/9.
 */
object GroupByTest {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


//    {"name":"Michael" ,"salary":50}
//    {"name":"Andy", "age":30,"salary":100}
//    {"name":"Flank", "age":19}
//
//    {"name":"Michael" ,"salary":150}
//    {"name":"Andy", "age":30,"salary":100}
//    {"name":"Flank", "age":19}
    val df = sqlContext.read.json("file/data/sql/person")


   val df2 = df.groupBy("name").sum("salary").withColumnRenamed("sum(salary)","sum")
    df2.show()
    //    |   name| sum|
    //    +-------+----+
    //    |   Andy| 200|
    //      |Michael| 200|
    //      |  Flank|null|
    //      +-------+----+
    val df3 = df.groupBy("name","age").sum("salary").withColumnRenamed("sum(salary)","sum")
    df3.show()
//    |   name| age| sum|
//    +-------+----+----+
//    |Michael|null| 200|
//      |  Flank|  19|null|
//      |   Andy|  30| 200|
//      +-------+----+----+


    /**
     *  val REMOVE_AMOUNT = order.groupBy("profile_id","state").agg(Map("sum"->"amount","count"->"profile_id"))
     *
     */
    val df4 = df.groupBy("name").agg(Map("salary"->"sum","name"->"count"))

    df4.show()
//    |   name|sum(salary)|count(name)|
//      +-------+-----------+-----------+
//    |   Andy|        100|          1|
//      |Michael|         50|          1|
//      |  Flank|       null|          1|
//      +-------+-----------+-----------+
    println("df5")
    val df5 = df.groupBy("name").count()
    df5.show()
//    +-------+-----+
//    |   name|count|
//    +-------+-----+
//    |   Andy|    1|
//    |Michael|    1|
//    |  Flank|    1|
//    +-------+-----+



    /*val df6 = df.count()
    df6.show()*/

  }

}
