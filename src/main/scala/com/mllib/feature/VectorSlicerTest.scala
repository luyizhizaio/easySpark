package com.mllib.feature

import java.util.Arrays
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by lichangyue on 2017/3/9.
 */
object VectorSlicerTest {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val data = Arrays.asList(Row(Vectors.dense(-2.0,2.3,0.0)))


    val defaultAttr = NumericAttribute.defaultAttr

    val attrs = Array("f1","f2","f3").map(defaultAttr.withName)

    attrs.foreach(println)
//    {"type":"numeric","name":"f1"}
//    {"type":"numeric","name":"f2"}
//    {"type":"numeric","name":"f3"}



    val attrGroup = new AttributeGroup("userFeatures",attrs.asInstanceOf[Array[Attribute]])
    val dataset = sqlContext.createDataFrame(data,StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))


    val output = slicer.transform(dataset)

    println(output.select("userFeatures","features").first())
//      [[-2.0,2.3,0.0],[2.3,0.0]]




    sc.stop()



  }


}
