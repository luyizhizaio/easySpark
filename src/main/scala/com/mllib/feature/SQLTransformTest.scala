package com.mllib.feature

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object SQLTransformTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    val df = sqlContext.createDataFrame(Seq((0,1.0,3.0),(2,2.0,5.0))).toDF("id","v1","v2")

    val sqlTrans = new SQLTransformer().setStatement(
    "SELECT *, (v1 + v2 ) as v3 ,(v1 * v2) as v4 FROM __THIS__")

    sqlTrans.transform(df).show()

//    +---+---+---+---+----+
//    | id| v1| v2| v3|  v4|
//    +---+---+---+---+----+
//    |  0|1.0|3.0|4.0| 3.0|
//      |  2|2.0|5.0|7.0|10.0|
//      +---+---+---+---+----+

  }

}
