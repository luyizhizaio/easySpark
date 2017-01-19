package com.ml.one.TransformerAndEstimaterAndPipeline

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{SQLContext, Row}
/**
 * Created by lichangyue on 2017/1/3.
 */
object MyPipeline {

  def main(args: Array[String]) {

    val conf = new SparkConf ()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // Prepare training documents from a list of (id, text, label) tuples.
    //准备训练文档，（id，内容，标签）
    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    //配置ML管道，由三个stage组成，tokenizer, hashingTF, and lr ，
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    //安装管道到数据上
    val model = pipeline.fit(training)

    // now we can optionally save the fitted pipeline to disk
    //现在可以保存安装好的管道到磁盘上
    model.save("/tmp/spark-logistic-regression-model")

    // we can also save this unfit pipeline to disk
    //也可以保存未安装的管道到磁盘上
    pipeline.save("/tmp/unfit-lr-model")

    // and load it back in during production
    //加载管道
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    //准备测试文档，不包含标签
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    //在测试文档上做出预测
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }

  }

}
