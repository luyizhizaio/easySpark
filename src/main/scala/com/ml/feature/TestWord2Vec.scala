package com.ml.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 每个文档中的词语转换成长度为3的向量：
 * Created by lichangyue on 2016/9/28.
 */
object TestWord2Vec {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setAppName("Word2Vec").setMaster("local")
    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits
    val docDF = sqlContext.createDataFrame(Seq(
      "苹果 官网 苹果 宣布".split(" "),
      "苹果 梨 香蕉".split(" ")
    ).map(Tuple1.apply)).toDF("text") //指定列名

    docDF.foreach(e => println(e))
//    [WrappedArray(苹果, 官网, 苹果, 宣布)]
//    [WrappedArray(苹果, 梨, 香蕉)]

    val word2Vec = new Word2Vec().setInputCol("text")
      .setOutputCol("result").setVectorSize(3).setMinCount(1)


    val model = word2Vec.fit(docDF)

    val result = model.transform(docDF)

    result.collect.foreach(println)
//    [WrappedArray(苹果, 官网, 苹果, 宣布),[0.006021047011017799,-0.002911671996116638,0.05357655562693253]]
//    [WrappedArray(苹果, 梨, 香蕉),[-0.10302492479483286,-0.059321289261182145,0.05107089380423228]]

  }

}
