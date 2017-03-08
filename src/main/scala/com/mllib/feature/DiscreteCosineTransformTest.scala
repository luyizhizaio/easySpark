package com.mllib.feature

import org.apache.spark.ml.feature.DCT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object DiscreteCosineTransformTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctdf = dct.transform(df)
    dctdf.select("featuresDCT").take(3).foreach(println)

//      [[1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604]]
//    [[-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677]]
//    [[4.0,9.304453421915744,11.000000000000002,1.5579302036357163]]

  }

}
