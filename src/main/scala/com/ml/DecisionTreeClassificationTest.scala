package com.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, VectorIndexer, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/16.
 */
object DecisionTreeClassificationTest {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val data  = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    //增加一列，给label增加一个索引列
    val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexedLabel")
    .fit(data)

    //自动处理离散型数值
    val featureInderxer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4) //特征超过四个值的当做连续型数值处理
    .fit(data)

    //切分数据
    val Array(trainingData, testData) = data.randomSplit(Array(0.7,0.3))

    //训练模型
    val dt = new DecisionTreeClassifier()
    .setLabelCol("indexedLabel")
    .setFeaturesCol("indexedFeatures")



    //把索引标签转成原始标签，和labelIndexer一起使用
    val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

    //设置管道
    val pipeline= new Pipeline()
    .setStages(Array(labelIndexer , featureInderxer,dt,labelConverter))

    //训练模型
    val model = pipeline.fit(trainingData)

    //做预测
    val predictions = model.transform(testData)

    //展示几行
    predictions.select("predictedLabel","label","features").show(5)

    //选择预测和真实标签 ，计算错误率
    val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("f1") //f1|precision|recall|weightedPrecision|weightedRecall)

    val f1 = evaluator.evaluate(predictions)
    println("f1 = " + f1)

    //得到分类模型

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println("learned classification tree model:\n" + treeModel.toDebugString)


    sc.stop()
  }

}
