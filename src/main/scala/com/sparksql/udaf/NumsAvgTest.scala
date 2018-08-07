package com.sparksql.udaf


import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType

/**
 * @author Administrator
 */
object NumsAvgTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._

    import sqlContext.implicits._

    val names = Array(("peter",""),("Leo","id3"), ("Marry","id2"), ("Jack","id2"), ("Tom","id2"), ("Tom","id1"), ("Tom","id2"), ("Leo","id1"))
    val numsDF = sc.parallelize(names, 1).toDF("mcId","idfa")


    numsDF.registerTempTable("numtest")

    sqlContext.udf.register("numsAvg", new NumsAvg)
    val xx = sqlContext.sql("select mcId, numsAvg(idfa) as idfa from numtest group by mcId ")

    xx.printSchema()

      xx.collect().foreach { x => println(x) }
  }
}

