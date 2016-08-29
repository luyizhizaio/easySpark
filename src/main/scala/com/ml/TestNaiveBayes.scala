package com.ml

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/26.
 */
object TestNaiveBayes {
  //样例类 不使用new关键字就可构建对象
  case class RawDataRecord(category: String, text:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TNBayes").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var srcRDD = sc.textFile("hdfs://S7SA053:8020/stat/sougou/").map(x =>{
      val data = x.split(",")
      RawDataRecord(data(0),data(1)) //返回结果
    })
    //70%作为训练数据，30%作为测试数据
    val splits= srcRDD.randomSplit(Array(0.7,0.3))
    var trainingDF = splits(0).toDF()  //转成DataFrame
    var testDF = splits(1).toDF()
    //将词语转成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1:")
    wordsData.select($"category",$"text",$"words").take(1)

    //计算每个词在文档中的词频
    var hashingTF = new  HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featureizedData = hashingTF.transform(wordsData) //转成词频
    println("output2:")

    featureizedData.select($"category",$"words",$"rawFeatures").take(1)

    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featureizedData)
    var rescaledData = idfModel.transform(featureizedData)
    println("output3:")
    rescaledData.select($"category",$"features").take(1)

    //转成bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map{
      case Row(label:String, features: Vector) =>
        LabeledPoint(label.toDouble,Vectors.dense(features.toArray))
    }
    println("output4:")
    trainDataRdd.take(1)

    //训练模型
    val model = NaiveBayes.train(trainDataRdd,lambda = 1.0,modelType="multinomial")
    //测试数据集，做同样的特征表示及格式转换

    var testwordsData = tokenizer.transform(testDF)
    var testFeaturizedData = hashingTF.transform(testwordsData)
    var testRescaledData = idfModel.transform(testFeaturizedData)
    var testDataRdd = testRescaledData.select($"category",$"features").map{
      case Row(label:String,features:Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }



    //对测试数据集使用训练模型进行分类预测,对模型进行校验
    val testpredictionAndLabel = testDataRdd.map(p=>(model.predict(p.features),p.label))

    //统计分类的准确率
    var testAccuracy = 1.0 * testpredictionAndLabel.
      filter( x => x._1 == x._2).count()/testDataRdd.count()

    println("output5:")
    println(testAccuracy)
  }
}
