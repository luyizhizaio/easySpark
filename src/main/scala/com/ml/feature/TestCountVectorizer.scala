package com.ml.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/28.
 */
object TestCountVectorizer {


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("TokenizerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(Seq(
      (0, Array("苹果","官网","苹果","宣布")),
      (1, Array("苹果","梨","香蕉"))
    )).toDF("id","words")


    var cvModel:CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(5) //设置词语的总个数，词语编号后的数值均小于该值
      .setMinDF(1) //设置包含词语的最少的文档数
      .fit(df)

    println("output1:")
    cvModel.transform(df).select("id","words","features").collect().foreach(println)
//    [0,WrappedArray(苹果, 官网, 苹果, 宣布),(5,[0,3,4],[2.0,1.0,1.0])]
//    [1,WrappedArray(苹果, 梨, 香蕉),(5,[0,1,2],[1.0,1.0,1.0])]

    var cvModel2: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)  //设置词语的总个数，词语编号后的数值均小于该值
      .setMinDF(2) //设置包含词语的最少的文档数 ,即一个词语至少要在两个两个文档中出现
      .fit(df)

    println("output2:")
    cvModel2.transform(df).select("id","words","features").collect().foreach(println)
//    [0,WrappedArray(苹果, 官网, 苹果, 宣布),(1,[0],[2.0])]
//    [1,WrappedArray(苹果, 梨, 香蕉),(1,[0],[1.0])]

    /**
     * 因为setMinDF(2)设置了词语最低出现的文档数为2，因此只保留了”苹果”一词。
     */
  }

}
