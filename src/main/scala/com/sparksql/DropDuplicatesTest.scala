package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/27.
 */
object DropDuplicatesTest {

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

    df.show()
    //    +----+-------+------+
    //    | age|   name|salary|
    //    +----+-------+------+
    //    |null|Michael|    50|
    //      |  30|   Andy|   100|
    //      |  19|  Flank|  null|
    //      |null|Michael|   150|
    //      |  30|   Andy|   100|
    //      |  19|  Flank|  null|
    //      +----+-------+------+


    val df1 = df.dropDuplicates() //对所有列进行去重
    df1.show()

    //
    //    +----+-------+------+
    //    | age|   name|salary|
    //    +----+-------+------+
    //    |  30|   Andy|   100|
    //      |  19|  Flank|  null|
    //      |null|Michael|   150|
    //      |null|Michael|    50|
    //      +----+-------+------+


    val df2 = df.dropDuplicates(Seq("name")) //对name列进行去重,其他列返回第一列
    df2.show()



    //
    //    +----+-------+------+
    //    | age|   name|salary|
    //    +----+-------+------+
    //    |  30|   Andy|   100|
    //      |null|Michael|    50|
    //      |  19|  Flank|  null|
    //      +----+-------+------+

    val df3 = df.dropDuplicates(Seq("name","age")) //对name列进行去重,其他列返回第一列
    df3.show()




//
//    +----+-------+------+
//    | age|   name|salary|
//    +----+-------+------+
//    |null|Michael|    50|
//      |  19|  Flank|  null|
//      |  30|   Andy|   100|
//      +----+-------+------+



  }

}
