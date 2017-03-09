package com.mllib.feature

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/9.
 */
object VectorAssemblyTest {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    val df= sqlContext.createDataFrame(Seq((0,18,1.9,Vectors.dense(0.0,10.0,0.5),1.0))
    ).toDF("id","hour","mobile","userFeatures","clicked")

    val assembler = new VectorAssembler()
    .setInputCols(Array("hour","mobile","userFeatures"))
    .setOutputCol("features")

    val out = assembler.transform(df)

    out.select("features","clicked").foreach(println)

//      [[18.0,1.9,0.0,10.0,0.5],1.0]

    sc.stop()


  }
}
