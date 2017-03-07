package com.mllib.feature

import org.apache.spark.ml.feature.{StopWordsRemover, RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lichangyue on 2017/3/7.
  */
object RemoveWordTest {

   def main(args: Array[String]) {


     val  conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)

     val remover = new StopWordsRemover()
     .setInputCol("raw")
     .setOutputCol("filtered")


     val dataSet = sqlContext.createDataFrame(Seq(
       (0,Seq("Hi","I","heard","the","red")),
       (1,Seq("Mary","had","a","little","lamb"))
     )).toDF("id","raw")

     remover.transform(dataSet).show()

    sc.stop()
   }

 }
