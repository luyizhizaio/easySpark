package com.mllib.feature

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 * 多元展开,
 */
object PolynomialExpansionTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    val data = Array(
      Vectors.dense(-2.0,2.3),
      Vectors.dense(0.0,0.0),
      Vectors.dense(0.6,-1.1)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val polynomialExpansion = new PolynomialExpansion()
    .setInputCol("features")
    .setOutputCol("polyFeatures")
    .setDegree(3)  //三度展开

    val polyDF= polynomialExpansion.transform(df)
    polyDF.select("polyFeatures").take(3).foreach(println)

//      [[-2.0,4.0,-8.0,2.3,-4.6,9.2,5.289999999999999,-10.579999999999998,12.166999999999996]]
//    [[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]]
//    [[0.6,0.36,0.216,-1.1,-0.66,-0.396,1.2100000000000002,0.7260000000000001,-1.3310000000000004]]

  }



  }
