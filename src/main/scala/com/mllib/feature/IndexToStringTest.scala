package com.mllib.feature

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object IndexToStringTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val df = sqlContext.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .fit(df)

    val indexed=indexer.transform(df)

    val converter = new IndexToString()
    .setInputCol("categoryIndex")
    .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)

    converted.select("id" , "originalCategory").show()

  }

}
