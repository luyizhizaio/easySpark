package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.MLUtils

/**
 * 专门讨论分布式矩阵
 * Created by lichangyue on 2016/9/7.
 */
object MLDataType2 {


  def main(args: Array[String]) {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);



    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TEST"))

    //1.行矩阵
    val rdd = sc.textFile("hdfs://S7SA053:8020/stat/MatrixRow.txt")
      .map(_.split(" ").map(_.toDouble)).map(line => Vectors.dense(line))

    val rm = new RowMatrix(rdd)
    println(rm.numRows())
    println(rm.numCols())
    rm.rows.foreach(println) //打印矩阵

    /** *
      输出结果：
      2
      3
      [1.0,2.0,3.0]
      [4.0,5.0,6.0]
      */

    //2.索引行矩阵

    println("Index row Matrix")
    val rdd2 = sc.textFile("hdfs://S7SA053:8020/stat/MatrixRow.txt")
    .map(_.split(" ").map(_.toDouble))
    .map(line => Vectors.dense(line))//转成向量
    .map((vd)=> new IndexedRow(vd.size,vd)) //转换成索引行

    val irm = new IndexedRowMatrix(rdd2) //创建索引行矩阵实例
    irm.rows.foreach(println)

    /**
     * IndexedRow(3,[1.0,2.0,3.0])
     * IndexedRow(3,[4.0,5.0,6.0])
     */


    //3.坐标矩阵
    val rdd3 = sc.textFile("hdfs://S7SA053:8020/stat/MatrixRow.txt")
    .map(_.split(" ").map(_.toDouble))
    .map(vue => (vue(0).toLong, vue(1).toLong,vue(2)))//转化成坐标格式
    .map(vue2 => new MatrixEntry(vue2._1,vue2._2 , vue2._3))//转化成坐标矩阵格式

    val crm = new CoordinateMatrix(rdd3) //实例化坐标矩阵
    crm.entries.foreach(println) //打印所有数据
    println(crm.numCols)
    println(crm.numRows())

    /**
    MatrixEntry(1,2,3.0)
MatrixEntry(4,5,6.0)
6
5
     */

    // 转换成索引行矩阵
    val indexedRowMatrix = crm.toIndexedRowMatrix()



    //4.块矩阵

    val rdd4 = sc.textFile("hdfs://S7SA053:8020/stat/MatrixRow.txt")
    .map(_.split(" ").map(_.toDouble))
    .map(vue => (vue(0).toLong,vue(1).toLong ,vue(2))) //转化成坐标格式
    .map(vue2 => new MatrixEntry(vue2._1 ,vue2._2,vue2._3))//转化成坐标矩阵格式

    val crm2 = new CoordinateMatrix(rdd4)

    val matA = crm2.toBlockMatrix().cache() //坐标转成块矩阵
    matA.validate()

    //矩阵相乘
    val ata = matA.transpose.multiply(matA)
    //转成坐标矩阵
    ata.toCoordinateMatrix().entries.foreach(println)
    //转成索引行矩阵
    ata.toIndexedRowMatrix().rows.foreach(println)

    /**
     *
     * MatrixEntry(2,2,9.0)
        MatrixEntry(5,5,36.0)
        IndexedRow(5,(6,[5],[36.0]))
        IndexedRow(2,(6,[2],[9.0]))
     */













  }




}
