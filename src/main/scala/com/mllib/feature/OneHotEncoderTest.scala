package com.mllib.feature

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object OneHotEncoderTest {

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

    val  indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")
    .fit(df)

    val indexed = indexer.transform(df)
    val encoder = new OneHotEncoder()
    .setInputCol("categoryIndex")
    .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    encoded.select("id","categoryVec").show()

// categoryVec 对应的是稀疏向量
//   +---+-------------+
//    | id|  categoryVec|
//    +---+-------------+
//    |  0|(2,[0],[1.0])|
//    |  1|    (2,[],[])|
//    |  2|(2,[1],[1.0])|
//    |  3|(2,[0],[1.0])|
//    |  4|(2,[0],[1.0])|
//    |  5|(2,[1],[1.0])|
//    +---+-------------+


  }


}
