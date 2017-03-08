package com.mllib.feature

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object BucketizerTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val splits = Array(Double.NegativeInfinity ,-0.5 ,0.0 ,0.5,Double.PositiveInfinity)

    val data = Array(-1, -0.5,-0.3 ,0.0,0.2 ,2.3 ,1000.0)

    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
    .setInputCol("features")
    .setOutputCol("bucketedFeatures")
    .setSplits(splits)

    val bucketedData = bucketizer.transform(df)

    bucketedData.show()
//    +--------+----------------+
//    |features|bucketedFeatures|
//    +--------+----------------+
//    |    -1.0|             0.0|
//      |    -0.5|             1.0|
//      |    -0.3|             1.0|
//      |     0.0|             2.0|
//      |     0.2|             2.0|
//      |     2.3|             3.0|
//      |  1000.0|             3.0|
//      +--------+----------------+

  }

}
