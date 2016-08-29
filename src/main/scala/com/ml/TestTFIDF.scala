package com.ml

import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/26.
 */
object TestTFIDF {

  case class RawDataRecord(category:String,text :String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TF-IDF").setMaster("local")

    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var srcDF = sc.textFile("").map{
      x =>
        var data =x.split(",")
        RawDataRecord(data(0),data(1))
    }.toDF()

    srcDF.select("category","text").take(2).foreach(println)

    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)

    wordsData.select($"category",$"text",$"words").take(2).foreach(println)

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var heaturizedData = hashingTF.transform(wordsData)




  }

}
