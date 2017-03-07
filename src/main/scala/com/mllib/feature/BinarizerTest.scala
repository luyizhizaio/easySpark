package com.mllib.feature

import org.apache.spark.ml.feature.{Binarizer, NGram}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lichangyue on 2017/3/7.
  */
object BinarizerTest {

   def main(args: Array[String]) {


     val  conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)


     val data = Array((0,0.1),(1,0.8),(2,0.2))
    val dataframe = sqlContext.createDataFrame(data).toDF("label","feature")

     val binarizer = new Binarizer()
     .setInputCol("feature")
     .setOutputCol("binarized_feature")
     .setThreshold(0.5)
     val binarizedDataFrame = binarizer.transform(dataframe)
     val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
     binarizedFeatures.collect().foreach(println)
    sc.stop()
   }

 }
