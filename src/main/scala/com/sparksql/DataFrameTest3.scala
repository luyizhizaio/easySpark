package com.sparksql

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.types.{StringType, StructField, IntegerType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/30.
 */
object DataFrameTest3 {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);



    val conf = new SparkConf().setAppName("text3").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val people =sc.textFile("hdfs://S7SA053:8020/stat/person.txt")

    val peopleRowRDD = people.map{x => x.split(",")}.map{data =>{
      val id = data(0).trim.toInt
      val name = data(1).trim
      val age = data(2).trim.toInt
      Row(id,name, age)
    }}

    val structType = StructType(Array(
      StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
    ))


    val df = sqlContext.createDataFrame(peopleRowRDD,structType)
    df.registerTempTable("people")

    df.show()
    df.printSchema()




  }

}
