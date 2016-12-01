package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 
 * Created by lichangyue on 2016/11/29.
 */
object DFJoin4RepeatColumnName {

  def main(args: Array[String]) {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //    {"name":"Michael" ,"salary":50}
    //    {"name":"Andy", "age":30,"salary":100}
    //    {"name":"Flank", "age":19}
    val df = sqlContext.jsonFile("file/data/sql/person")

    //    {"name":"Peter","id":20 }
    //    {"name":"Andy", "id":31}
    //    {"name":"Justin"}
    val df2 = sqlContext.jsonFile("file/data/sql/person2")


    val joindf = df.join(df2,Seq("name"))
    joindf.show()
    /**
+----+---+------+---+
|name|age|salary| id|
+----+---+------+---+
|Andy| 30|   100| 31|
+----+---+------+---+
     */

    val Leftjoindf = df.join(df2,Seq("name") , "left")
    Leftjoindf.show()
    /**
+-------+----+------+----+
|   name| age|salary|  id|
+-------+----+------+----+
|Michael|null|    50|null|
|   Andy|  30|   100|  31|
|  Flank|  19|  null|null|
+-------+----+------+----+
     */


  }

}
