package com.ml.extractAndtransformerAndSelect
import org.apache.spark.ml.feature.Word2Vec
/**
 * Created by lichangyue on 2017/1/3.
 */
object MyWord2Vct {

  def main(args: Array[String]) {



    // Input data: Each row is a bag of words from a sentence or document.
    //输入数据:每一行是一个单词集合，来自一句话或一个文档
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)



  }

}
