package com.sparksql

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/8/30.
 */
object DataFrameTest2 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    var conf = new SparkConf().setAppName("test2").setMaster("local")
    var sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val
    df = sqlContext.read.json("hdfs://S7SA053:8020/stat/person.json")

    df.registerTempTable("people")
    //查看df中的数据
    df.show()
    df.printSchema()


    sqlContext.sql("select name from people ").show()

    sqlContext.sql("select name,age+1 from people where age >=25").show()

    //count group by
    sqlContext.sql("select age,count(*) cnt from people group by age").show()

    sqlContext.sql("select id ,name,age from people").foreach(x =>{

      println("col1:" +x.get(0)+ "col2:"+x.get(1) + "col3:"+ x.get(3))
    })

    sqlContext.sql("select id,name age from people ").foreachPartition(iterator =>{
      iterator.foreach(x =>{
        println("col1:" +x.getAs("id")+ "col2:"+x.getAs("name") + "col3:"+ x.getAs("age"))
      })

    })



  }

}
