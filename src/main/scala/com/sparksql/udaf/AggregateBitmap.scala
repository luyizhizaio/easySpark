package com.sparksql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author tend
  */
class AggregateBitmap extends UserDefinedAggregateFunction {
   //inputSchema，指的是，输入数据的类型
   def inputSchema: org.apache.spark.sql.types.StructType =
     StructType(StructField("str", StringType,true) :: Nil)

   // bufferSchema，指的是，中间进行聚合时，所处理的数据的类型
   def bufferSchema: StructType = StructType(
     StructType(Array(StructField("append", StringType, true)))
   )

   // dataType，指的是，函数返回值的类型
   def dataType: DataType = StringType

   def deterministic: Boolean = true

   // 为每个分组的数据执行初始化操作
   def initialize(buffer: MutableAggregationBuffer): Unit = {
     buffer(0) = ""
   }

   // 指的是，每个分组，有新的值进来的时候，如何进行分组对应的聚合值的计算
   def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
     val a= input.getString(0).split("\\|").toSet
     val b = buffer.getString(0).split("\\|").toSet
     val c =a ++ b
     buffer(0) = c.mkString("|")

   }

   // 由于Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
   // 但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
   def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
     val a= buffer1.getString(0).split("\\|").toSet
     val b = buffer2.getString(0).split("\\|").toSet
     val c =a ++ b
     buffer1(0) = c.filter(_ !="").mkString("|")
   }

   // 最后，指的是，一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
   def evaluate(buffer: Row): Any = {

     buffer.getString(0)

   }
 }
