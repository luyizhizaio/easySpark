package com.mllib.feature

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object StringIndexerTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id","category")

    val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)

    indexed.show()
//    +---+--------+-------------+
//    | id|category|categoryIndex|
//    +---+--------+-------------+
//    |  0|       a|          0.0|
//      |  1|       b|          2.0|
//      |  2|       c|          1.0|
//      |  3|       a|          0.0|
//      |  4|       a|          0.0|
//      |  5|       c|          1.0|
//      +---+--------+-------------+
  }

}
