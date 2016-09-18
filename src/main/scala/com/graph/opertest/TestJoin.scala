package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 例子说明：利用joinVertices和outJoinVertices对graph的顶点属性进行修改
 * Created by lichangyue on 2016/9/18.
 */
object TestJoin {


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf  = new SparkConf()
    val sc = new SparkContext ("local","test",conf)

    // 利用edge信息生成图
    // dataset info
    // 1 2
    // 2 3
    // 3 1
    val graph = GraphLoader.edgeListFile(sc, "hdfs://S7SA053:8020/stat/join-edges.tsv").cache()


    // 以[vid, name]形式读取vertex信息
    // day03-vertices.csv
    // 1,Taro
    // 2,Jiro
    val vertexLines = sc.textFile("hdfs://S7SA053:8020/stat/join-vertices.tsv")
    val users = vertexLines.map(line =>{
      val cols = line.split(",")
      (cols(0).toLong, cols(1))
    })

    // 将users中的vertex属性添加到graph中，生成graph2
    // 使用joinVertices操作，根据id进行连接,用user中的属性替换图中对应Id的属性
    // 先将图中的顶点属性置空
    //((vid,attr,user) =>user) 是个map函数
    val graph2 = graph.mapVertices((id,attr) => "").joinVertices(users)(
      (vid,attr,user) =>user)

    println("\n\nConfirm vertices Internal of graph2")
    graph2.vertices.collect().foreach(println(_))
//    (1,Taro  )
//    (3,)
//    (2,Jiro)


    // 使用outerJoinVertices将user中的属性赋给graph中的顶点，如果图中顶点不在user中，则赋值为None

    val graph3 = graph.mapVertices((id,attr)=> "").outerJoinVertices(users){
      (vid,attr,user) => user.getOrElse("None")}

    println("\n\nconfirm vertices Internal of graph3 ")
    graph3.vertices.collect.foreach(println(_))
    //(1,Taro  )
    //(3,None)
    //(2,Jiro)
    // 结果表明，如果graph的顶点在user中，则将user的属性赋给graph中对应的顶点，否则赋值为None
    sc.stop()
  }

}
