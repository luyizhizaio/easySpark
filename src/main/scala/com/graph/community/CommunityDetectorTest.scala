package com.graph.community

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dayue on 2017/2/23.
 */
object CommunityDetectorTest extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


  val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

  val sc = new SparkContext(conf)

  val data = sc.textFile("file/temp/0.edges")

  val adjMatrixEntry = data.map(_.split(" ") match { case Array(id1 ,id2) =>
    MatrixEntry(id1.toLong-1,id2.toLong -1 , -1.0)
  })
  //邻接矩阵
  val adjMatrix = new CoordinateMatrix(adjMatrixEntry)
  println("Num of nodes=" + adjMatrix.numCols() +", Num of edges=" +data.count())
//  Num of nodes=347, Num of edges=5038

  //2.计算拉普拉斯矩阵
  //
  val rows = adjMatrix.toIndexedRowMatrix().rows

  val diagMatrixEntry = rows.map{row=>
    MatrixEntry(row.index ,row.index, row.vector.toArray.sum)
  }
  //计算拉普拉斯矩阵 L =D-S
  val laplaceMatrix = new CoordinateMatrix(sc.union(adjMatrixEntry, diagMatrixEntry))

  //计算拉普拉斯矩阵的特征列向量构成的矩阵(假设聚类个数是5)
  val eigenMatrix = laplaceMatrix.toRowMatrix().computePrincipalComponents(5)

  val nodes = eigenMatrix.transpose.toArray.grouped(5).toSeq
  val nodeSeq = nodes.map(node =>Vectors.dense(node))

  val nodeVectors = sc.parallelize(nodeSeq)


  //求解在新向量表示下的K均值聚类结果

  val clusters = new KMeans()
    .setK(5)
    .setMaxIterations(100) //最大迭代次数
    .run(nodeVectors)

  val result = clusters.predict(nodeVectors).zipWithIndex().groupByKey().sortByKey()

  result.collect.foreach{c =>
    println("Nodes in cluster" +(c._1 + 1) + ": ")
    c._2.foreach(n=>print(" " +n))
    println()
  }
  sc.stop()

}
