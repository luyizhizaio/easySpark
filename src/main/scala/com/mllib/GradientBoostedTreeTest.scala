package com.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by lichangyue on 2017/1/19.
 */
object GradientBoostedTreeTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    //本地文件
    val ctrRDD = sc.textFile("file:///root/lichangyue/train.csv",10)

    //将整个数据集80%作为训练数据，20%作为测试数据集
    val rdd = ctrRDD.randomSplit(Array(0.8,0.2),seed=37L)
    val train_raw_rdd = rdd(0).cache()
    val test_raw_rdd = rdd(1).cache()

    val train_rdd = train_raw_rdd.map{line=>
      val tokens = line.split(",",-1)
      //key 由id和是否点击广告组成
      val catkey = tokens(0) +"::" + tokens(1)
      //第6列到第15列为分类特征，需要One-Hot-Encoding
      val catfeatures = tokens.slice(5,14)
      //第16列到24列为数值特征，直接使用
      val numericalFeatures = tokens.slice(15,tokens.size -1)
      (catkey , catfeatures , numericalFeatures)

    }

    train_raw_rdd.take(1).foreach(println)
//    1000009418151094273,0,14102100,1005,0,1fbe01fe,f3845767,28905ebd,ecad2386,7801e8d9,07d7df22,a99f214a,ddd2926e,44956a24,1,2,15706,320,50,1722,0,35,-1,79

    //将分类特征先做特征ID映射
    val train_cat_rdd = train_rdd.map{
      x => parseCatFeatures(x._2)
    }
    train_cat_rdd.take(1).foreach(println)
//    List((0,1fbe01fe), (1,f3845767), (2,28905ebd), (3,ecad2386), (4,7801e8d9), (5,07d7df22), (6,a99f214a), (7,ddd2926e), (8,44956a24))


    //将train_cat_rdd中的(特征ID：特征)去重，并进行编号
    val oheMap = train_cat_rdd.flatMap(x=>x).distinct().zipWithIndex().collectAsMap()

    println("Number of features")
    println(oheMap.size)

    val ohe_train_rdd = train_rdd.map{case (key, cateorical_featuers,numerical_fueatures) =>
      val cat_features_indexed = parseCatFeatures(cateorical_featuers)

      val cat_feature_ohe = new ArrayBuffer[Double]
      for(k <- cat_features_indexed){
        if(oheMap contains k){
          cat_feature_ohe += (oheMap get (k)).get.toDouble
        } else {
          cat_feature_ohe +=0.0
        }
      }
      //数值特征处理
      val numerical_features_dbl = numerical_fueatures.map{
        x =>
          var x1 = if(x.toInt < 0) "0" else x
          x1.toDouble
      }
      val features = cat_feature_ohe.toArray ++ numerical_features_dbl
      LabeledPoint(key.split("::")(1).toInt,Vectors.dense(features))
    }

    ohe_train_rdd.take(1).foreach(println)
    //训练模型
    //val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(100)
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(10)
    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int,Int]())

    val model = GradientBoostedTrees.train(ohe_train_rdd , boostingStrategy)

    //保存模型
    model.save(sc, "/tmp/myGradientBoostingClassificationModel")
    //加载模型
    val sameModel = GradientBoostedTreesModel.load(sc,"/tmp/myGradientBoostingClassificationModel")

    //将测试数据集做OHE
    var test_rdd = test_raw_rdd.map{ line =>
      var tokens = line.split(",")
      var catkey = tokens(0) + "::" + tokens(1)
      var catfeatures = tokens.slice(5, 14)
      var numericalfeatures = tokens.slice(15, tokens.size-1)
      (catkey, catfeatures, numericalfeatures)
    }

    var ohe_test_rdd = test_rdd.map{ case (key, cateorical_features, numerical_features) =>
      var cat_features_indexed = parseCatFeatures(cateorical_features)
      var cat_feature_ohe = new ArrayBuffer[Double]
      for (k <- cat_features_indexed) {
        if(oheMap contains k){
          cat_feature_ohe += (oheMap get (k)).get.toDouble
        }else {
          cat_feature_ohe += 0.0
        }
      }
      var numerical_features_dbl  = numerical_features.map{x =>
        var x1 = if (x.toInt < 0) "0" else x
        x1.toDouble}
      var features = cat_feature_ohe.toArray ++  numerical_features_dbl
      LabeledPoint(key.split("::")(1).toInt, Vectors.dense(features))
    }


    //验证测试数据集
    val b = ohe_test_rdd.map{ y =>
      val s =model.predict(y.features)
      (s,y.label,y.features)
    }

    b.take(10).foreach(println)
    //预测准确率
    val predictions = ohe_test_rdd.map(lp => model.predict(lp.features))

    val predictionsAndLabel = predictions.zip(ohe_test_rdd.map(_.label))

    val accuracy = 1.0 * predictionsAndLabel.filter(x => x._1 == x._2).count/ohe_test_rdd.count
    println("GBTR accuracy:" + accuracy)





  }

  //input (1fbe01fe,f3845767,28905ebd,ecad2386,7801e8d9)
  //output ((0:1fbe01fe),(1:f3845767),(2:28905ebd),(3:ecad2386),(4:7801e8d9))
  def parseCatFeatures(catfeatures :Array[String]) :List[(Int,String)] ={
    val catfeatureList = new ListBuffer[(Int,String)]()

    for(i <- 0 until catfeatures.length){
      catfeatureList += i -> catfeatures(i).toString
    }
    catfeatureList.toList
  }


}
