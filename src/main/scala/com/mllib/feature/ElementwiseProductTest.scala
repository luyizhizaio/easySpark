package com.mllib.feature

import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object ElementwiseProductTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)




    val df = sqlContext.createDataFrame(Seq(
      ("a",Vectors.dense(1.0,2.0,3.0)),
      ("a",Vectors.dense(4.0,5.0,6.0))
    )).toDF("id","vector")

    val transformingVector = Vectors.dense(0.0,2.0,3.0)

    val transformer = new ElementwiseProduct()
    .setScalingVec(transformingVector)
    .setInputCol("vector")
    .setOutputCol("transformedVector")

    transformer.transform(df).show()

//    +---+-------------+-----------------+
//    | id|       vector|transformedVector|
//    +---+-------------+-----------------+
//    |  a|[1.0,2.0,3.0]|    [0.0,4.0,9.0]|
//    |  a|[4.0,5.0,6.0]|  [0.0,10.0,18.0]|
//    +---+-------------+-----------------+




  }


}
