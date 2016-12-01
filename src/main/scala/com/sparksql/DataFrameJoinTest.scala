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


//    {"name":"Michael" ,"salary":50}
//    {"name":"Andy", "age":30,"salary":100}
//    {"name":"Flank", "age":19}
    val df = sqlContext.jsonFile("file/data/sql/person").withColumnRenamed("name","name1")

//    {"name":"Peter","id":20 }
//    {"name":"Andy", "id":31}
//    {"name":"Justin"}
    val df2 = sqlContext.jsonFile("file/data/sql/person2")


    val joindf = df.join(df2, df("name1") === df2("name"))

    joindf.show()

    /**
+---+----+------+---+----+
|age|name|salary| id|name|
+---+----+------+---+----+
| 30|Andy|   100| 31|Andy|
+---+----+------+---+----+
     */

    joindf.printSchema()



    /**
 root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: long (nullable = true)
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
     */
    joindf.select(joindf("name1").substr(1,2).alias("subname"),joindf("name1").substr(3,2).alias("wwwname") ,joindf("name1") ).show


  }


}
