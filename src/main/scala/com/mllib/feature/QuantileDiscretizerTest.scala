package com.mllib.feature

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/9.
 */
object QuantileDiscretizerTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val data = Array((0,18.0),(1,19.0),(2,8.0),(3,5.0),(4,2.2))

    val df = sqlContext.createDataFrame(data).toDF("id","hour")

    val discretizer = new QuantileDiscretizer()
    .setInputCol("hour")
    .setOutputCol("result")
    .setNumBuckets(3)
    discretizer.fit(df).transform(df).show()

//    +---+----+------+
//    | id|hour|result|
//    +---+----+------+
//    |  0|18.0|   2.0|
//      |  1|19.0|   2.0|
//      |  2| 8.0|   1.0|
//      |  3| 5.0|   1.0|
//      |  4| 2.2|   0.0|
//      +---+----+------+
  }

}
