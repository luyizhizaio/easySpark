package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/11/9.
 */
object DataFrameAboutSelect {


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

    val fuck = person.select(person("name").startsWith("Fuck_").as("name1") ,person("name"))
    fuck.show()
  }


}
