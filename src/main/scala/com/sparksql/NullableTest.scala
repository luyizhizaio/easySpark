package com.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/12/28.
 */
object NullableTest {

  /**
   * 空值处理
    * @param args
   */
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("dataFrametest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


//    {"name":"Michael" ,"salary":50}
//    {"name":"Andy", "age":30,"salary":100}
//    {"name":"Flank", "age":19}
    val df = sqlContext.read.json("file/data/sql/person").select("name")

//    {"name":"Peter","id":20 }
//    {"name":"Andy", "id":31}
//    {"name":"Justin"}
    val df2 = sqlContext.read.json("file/data/sql/person2")



    //case  when (tmp.type1 ='12')  then 'phone' when (tmp.type1 ='11')  then 'account' when (tmp.type1 = '17') then 'ufpd' else '' end,
    val df3 = df.join(df2,Seq("name"),"left")



    df3.printSchema()

    df3.show()
   /* df3.foreach(row=> {

      if(row.get(1) == null) println("null") else println("id:"+row.getAs[Int](1))
    })*/

    val df4 = df3.selectExpr("name" ,"case when id is null then 0 else id end as id ")
    df4.show()

    sc.stop()


  }



}
