package com.mllib.feature

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/7.
 */
object TokenizerTest {

  def main(args: Array[String]) {


    val  conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0,"Hi I heard about Spark"),
      (1,"I wish java could use case classes"),
      (2,"Logistic,regression,models,are,neat")
    )).toDF("label","sentence")

    //默认空格分割
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words","label").take(3).foreach(println)


    println("-------------------")
    var regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words")
      .setPattern("\\W")
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("words","label").take(3).foreach(println)

    println("-------------------")
    var regexTokenizer2 = new RegexTokenizer().setInputCol("sentence").setOutputCol("words")
      .setPattern("\\w+").setGaps(false)

    val regexTokenized2 = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized2.select("words","label").take(3).foreach(println)

  }

}
