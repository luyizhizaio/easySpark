package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/16.
 */
object MLPTest {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf().setAppName("decisionTree").setMaster("local[2]")


    val sc  = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_multiclass_classification_data.txt")

    val  Array(train,test) = data.randomSplit(Array(0.6, 0.4),seed=1234L)

    //指定神经网络的层数：
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)

    val layer =Array[Int](4,5,4,3)

    val trainer = new  MultilayerPerceptronClassifier()
    .setLayers(layer)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)

    //训练模型
    val model = trainer.fit(train)

    //测试集上计算精度
    val result = model.transform(test)

    val predictAndLabels = result.select("prediction","label")
    val evaluator = new MulticlassClassificationEvaluator()
    .setMetricName("f1")

    println("f1:" +evaluator.evaluate(predictAndLabels))

  }

}
