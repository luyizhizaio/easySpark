package com.sparksql.udaf


import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable

/**
 * @author Administrator
 */
object NumsAvgTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")

    val idtype = "idfa:imei"

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._

    import sqlContext.implicits._

    val names = Array((1L,"peter","peter",""),(2L,"Leo","Leo",""),
      (3L,"Marry","","Marry"), (4L,"Jack","","Jack"),
      (5L,"Tom","Tom",""), (6L,"id1","id1",""),
      (5L,"Tom","Tom",""), (2L,"Leo","","Leo"),
      (2L,"Leo","Leo",""))
    val numsDF = sc.parallelize(names, 1).toDF("offset","mcId","idfa","imei")

    val names2 = Array((10L,"peter2","","peter2"),(11L,"Leo2","","Leo2"))


    val numsDF2 = sc.parallelize(names2, 1).toDF("offset","idfa","imei","mcId")

    numsDF2.show(20,false)

    numsDF.unionAll(numsDF2).show(20,false)



    val rdd = numsDF.map{row =>
      val offset = row.getLong(row.fieldIndex("offset"))
      val mcId = row.getString(row.fieldIndex("mcId"))

      val xx= idtype.split(":").map{item =>
        row.getString(row.fieldIndex(item))
      }.mkString(",")

      s"$offset,$mcId" -> List(xx)
    }.reduceByKey(_ ++ _).map{ case(offsetMcId,arr)=>
      val length = idtype.split(":").length

      val ids = (0 to length -1).map{index =>
        val set = mutable.Set[String]()
        arr.foreach{line =>
          val idArr = line.split(",",length)
          set.add(idArr(index))
        }
        set.filter{_ !=""}.mkString("|")
      }.mkString(",")
      val Array(offset,mcId) = offsetMcId.split(",",2)
      s"$offset,$ids,$mcId"
    }


    rdd.take(10).foreach(println)




    numsDF.registerTempTable("numtest")

    sqlContext.udf.register("numsAvg", new NumsAvg)
    sqlContext.udf.register("arrayAvg", new ArrayAvg)

    val idsql = idtype.split(":").map { item =>
      s"arrayAvg(${item}) as $item"
      //1.保存大库中 offset和mcId对应关系
    }.mkString(",")

    val dakuSql = s"select offset,mcId, ${idsql} from  numtest group by offset, mcId"

    println("dakusql : "+dakuSql)

    val xx = sqlContext.sql(dakuSql)

    xx.printSchema()

    xx.show(20,false)

  }
}

