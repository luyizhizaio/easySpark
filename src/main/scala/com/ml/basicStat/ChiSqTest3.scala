package com.ml.basicStat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.stat.Statistics

/**
 * Created by lichangyue on 2016/9/21.
 */
object ChiSqTest3 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val vd = Vectors.dense(1,2,3,4,5)
    val vdResult = Statistics.chiSqTest(vd)
    println("vectors chisqTest:~~~~~~~~")
    println(vdResult)
//    Chi squared test summary:
//      method: pearson
//    degrees of freedom = 4
//    statistic = 3.333333333333333
//    pValue = 0.5036682742334986
//    No presumption against null hypothesis: observed follows the same distribution as expected..

    val mtx =Matrices.dense(3,2,Array(1,3,5,2,4,6))
    val mtxResult = Statistics.chiSqTest(mtx)

    println("mtx chisqtest:~~~~~~~")
    println(mtxResult)
//    Chi squared test summary:
//      method: pearson
//    degrees of freedom = 2
//    statistic = 0.14141414141414144
//    pValue = 0.931734784568187
//    No presumption against null hypothesis: the occurrence of the outcomes is statistically independent..

  }

}
