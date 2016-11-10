package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/11/8.
 */
object DataFrameJoinTest {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.jsonFile("file/data/sql/person")

    sqlContext.read.json()


    val df2 = sqlContext.jsonFile("file/data/sql/person2")


    val joindf = df.join(df2, df("name") === df2("name"))

    joindf.show()

    joindf.printSchema()

  }


}
