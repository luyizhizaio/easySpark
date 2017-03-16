package com.mllib

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vector
/**
 * Created by lichangyue on 2017/3/15.
 */
object PipelineTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)


    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    //变成字符串数组
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000) //hashMap的长度
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()

    //输出模型的默认参数值
    lr.explainParams()

    //设置参数
    lr.setMaxIter(10)
    .setRegParam(0.01)


    val pipeline = new Pipeline()
      .setStages(Array(tokenizer,hashingTF,lr)) //设置各个Stage

    val pipelineModel = pipeline.fit(training)

//    pipelineModel.save("/tmp/spark-lr")
//    val sameModel = PipelineModel.load("/tmp/spark-lr")

    //测试数据
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")


    //在测试集上做预测
    val result = pipelineModel.transform(test)


    result.printSchema()

    result.select("id","text","probability","prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }

//输出结果：
//    (4, spark i j k) --> prob=[0.5406433544851431,0.45935664551485683], prediction=0.0
//    (5, l m n) --> prob=[0.9334382627383263,0.06656173726167372], prediction=0.0
//    (6, mapreduce spark) --> prob=[0.7799076868203894,0.2200923131796106], prediction=0.0
//    (7, apache hadoop) --> prob=[0.9768636139518304,0.023136386048169637], prediction=0.0


  sc.stop()
  }

}
