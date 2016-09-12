package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/5.
 */
object TestDecisionTree2 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    val conf = new SparkConf().setAppName("decisionTree").setMaster("local[2]")

    val sc = new SparkContext(conf)
    //训练数据
    val data1 = sc.textFile("hdfs://S7SA053:8020/stat/tree1")
    //测试数据
    val data2 = sc.textFile("hdfs://S7SA053:8020/stat/tree2")
    //转换成向量
    val tree1 = data1.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))

    })


    val tree2 = data2.map(line =>{
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))

    })
    //赋值
    val (trainData,testData) = (tree1,tree2)

    //分类数量
    val numClasses =2
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity ="gini"
    //最大深度
    val maxDepth = 5
    //最大分值
    val maxBins =32
    //模型训练
    val model = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,
      impurity,maxDepth,maxBins)

    //模型预测
    val labeAndPreds = testData.map{
      point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    //测试值与真实值对比
    val print_predict = labeAndPreds.take(15)
    println("label"+"\t"+"prediction")

    for(i <- print_predict){
      println(i._1 +"\t"+ i._2)
    }

    //树的错误率
    val testErr = labeAndPreds.filter(r => r._1 !=r._2).count.toDouble/testData.count()
    println("test error:" + testErr)
    //树的判断值
    println("learned classfifcation tree model"+model.toDebugString)

  }

}
