package com.mllib.feature

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2017/3/8.
 */
object MinMaxScaler {


  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val df = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new MinMaxScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")

    //计算汇总统计量，返回一个模型
    val scalerModel = scaler.fit(df)

    //标准化每个特征
    val scaledData = scalerModel.transform(df)

    scaledData.show()





  }

}
