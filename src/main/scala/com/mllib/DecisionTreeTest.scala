package com.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/1/19.
 */
object DecisionTreeTest {


  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("regression").setMaster("local[1]")
    val sc=new SparkContext(conf)

//    1,2011-01-01,1,0,1,0,0,6,0,1,0.24,0.2879,0.81,0,3,13,16
    val records=sc.textFile("data/mllib/hour.csv").map(_.split(",")).cache()

    //特征提取
    val data = records.map{ record =>
      val features = record.slice(2,14).map(_.toDouble) //切取数组
      val label = record(record.size -1).toDouble
      LabeledPoint(label, Vectors.dense(features))
    }.randomSplit(Array(0.7,0.3),11L)

    val train_data =data(0)
    val test_data = data(1)



    val categoricalFeaturesInfo = Map[Int,Int]()
    //训练模型
    val  treeModel = DecisionTree.trainRegressor(train_data,categoricalFeaturesInfo,"variance",5,32)
    //预测
    val true_vs_predicted = test_data.map(p =>(p.label,treeModel.predict(p.features)))

    println(true_vs_predicted.take(5).toVector.toString())

    //性能评估
    //两个值差平方的均值
    val MSE = true_vs_predicted.map(value=>{
      (value._1 -value._2) * (value._1 -value._2)
    }).mean()

    //两个值差绝对值的均值
    val MAE = true_vs_predicted.map(value =>{
      math.abs(value._1-value._2)
    }).mean()

    //预测值和目标值进行对数变换后的RMSE.
    val RMSLE = true_vs_predicted.map(value =>{

      math.pow(math.log(value._1 + 1) - math.log(value._2 +1),2)
    }).mean()

    println("MSE" + MSE)
    println("MAE" + MAE)
    println("RMSLE" + RMSLE)

  }

}
