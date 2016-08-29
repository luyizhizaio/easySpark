package com.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/26.
 */
object DataFrameTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://S7SA053:8020/stat/person.json")
    //查看df中的数据
    df.show();
    //查看schema
    df.printSchema()
    //
    df.select("name").show()

    df.select(df.col("name"),df.col("age").plus(1)).show()

    df.filter(df.col("age").gt(25)).show()

    df.groupBy("age").count().show()





  }

}
