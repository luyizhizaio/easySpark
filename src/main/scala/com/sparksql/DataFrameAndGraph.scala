package com.sparksql


import breeze.linalg.{sum, min, max}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/11/9.
 */
object DataFrameAndGraph {


  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /**
     * {"name":"Michael" ,"salary":50}
     * {"name":"Andy", "age":30,"salary":100}
     * {"name":"Flank", "age":19}
     */
    val person = sqlContext.read.json("file/data/sql/person")

    person.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)


    val groupPerson = person.groupBy("name").count()

    groupPerson.show()
  /*+-------+-----+
    |   name|count|
    +-------+-----+
    |   Andy|    1|
    |Michael|    1|
    |  Flank|    1|
    +-------+-----+*/


    /**
     1.df.agg(...) is a shorthand for df.groupBy().agg(...)
df.agg(max($"age"), avg($"salary"))
df.groupBy().agg(max($"age"), avg($"salary"))
     */

    val c = person("age")+ 10
    println(c)

//2.Map参数
    //df.agg(Map("age" -> "max", "salary" -> "avg"))
    //df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
    val agg1 = person.agg(Map("age" -> "max", "salary" -> "avg"))
    agg1.show()

    agg1.printSchema()

    val agg2 = person.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
    agg2.show()


    val agg3 = person.groupBy("name").agg(Map("age" -> "max", "salary" -> "avg"))
    agg3.show()


/*

    +-------+-----+
    |   name|count|
    +-------+-----+
    |   Andy|    1|
      |Michael|    1|
      |  Flank|    1|
      +-------+-----+


    +--------+-----------+
    |max(age)|avg(salary)|
      +--------+-----------+
    |      30|       75.0|
      +--------+-----------+

    root
    |-- max(age): long (nullable = true)
    |-- avg(salary): double (nullable = true)


    +--------+-----------+
    |max(age)|avg(salary)|
      +--------+-----------+
    |      30|       75.0|
      +--------+-----------+


    +-------+--------+-----------+
    |   name|max(age)|avg(salary)|
      +-------+--------+-----------+
    |   Andy|      30|      100.0|
      |Michael|    null|       50.0|
      |  Flank|      19|       null|
      +-------+--------+-----------+*/
  }


}
