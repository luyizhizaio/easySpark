package com.ml.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/9/28.
 */
object TestTFIDF {
  case class RawDataRecord(category:String, text:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("TestTFIDF").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext =new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

//    0,苹果 官网 苹果 宣布
//    1,苹果 梨 香蕉
    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var srcDF = sc.textFile("file/temp/1.txt").map{
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }.toDF()

    srcDF.select("category","text").take(2).foreach(println)
//    [0,苹果 官网 苹果 宣布]
//    [1,苹果 梨 香蕉]

    //将分好的词转换为数组
    //A tokenizer that converts the input string to lowercase and then splits it by white spaces.
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)

    wordsData.select($"category",$"text",$"words").take(2).foreach(println)
//      [0,苹果 官网 苹果 宣布,WrappedArray(苹果, 官网, 苹果, 宣布)]
//    [1,苹果 梨 香蕉,WrappedArray(苹果, 梨, 香蕉)]

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    .setNumFeatures(100)
    /*
    这里将中文词语转换成INT型的Hashing算法，类似于Bloomfilter，
    上面的setNumFeatures(100)表示将Hash分桶的数量设置为100个，这个值默认为2的20次方，即1048576，
    可以根据你的词语数量来调整，一般来说，这个值越大，不同的词被计算为一个Hash值的概率就越小，数据也更准确，
    但需要消耗更大的内存，和Bloomfilter是一个道理。
     */
    var featurizedData = hashingTF.transform(wordsData)



    featurizedData.select("category","words","rawFeatures").take(2).foreach(println)
    //    [0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[2.0,1.0,1.0])]
    //    [1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[1.0,1.0,1.0])]


    //计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println(rescaledData)
    rescaledData.select("category","words","features").take(2).foreach(println)
//      [0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[0.0,0.4054651081081644,0.4054651081081644])]
//    [1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[0.0,0.4054651081081644,0.4054651081081644])]

    /**
     *因为一共只有两个文档，且都出现了“苹果”，因此该词的TF-IDF值为0.
     */


    //最后一步，将上面的数据转换成Bayes算法需要的格式

    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }


    trainDataRdd.take(2).foreach(println)
//    (0.0,[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.4054651081081644,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.4054651081081644,0.0,0.0,0.0])
//    (1.0,[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.4054651081081644,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.4054651081081644,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
      /*
      *每一个LabeledPoint中，
      * 特征数组的长度为100（setNumFeatures(100)），”官网”和”宣布”对应的特征索引号分别为81和96，
      * 因此，在特征数组中，第81位和第96位分别为它们的TF-IDF值。
* */

  }
}
