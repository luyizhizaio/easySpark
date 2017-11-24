package com.graph.opertest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 假定我们想从一些文本文件中构建一个图，限制这个图包含重要的关系和用户，
 * 并且在子图上运行page-rank，最后返回与top用户相关的属性。
 * 可以通过如下方式实现。
 * Created by lichangyue on 2016/9/13.
 */
object GraphOfficalSample13 {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val sc = new SparkContext(new SparkConf()
      .setAppName("pageRank").setMaster("local"))

    val usersRDD = sc.textFile("hdfs://S7SA053:8020/stat/users.txt")

    //生用户id和属性列表的元组
    val users = usersRDD.map(line => line.split(","))
    .map(parts => (parts.head.toLong,parts.tail))

    val followerGraph = GraphLoader.edgeListFile(sc,"hdfs://S7SA053:8020/stat/followers.txt")

    //绑定用户属性
    val graph = followerGraph.outerJoinVertices(users){
      case(uid,deg, Some(attrList)) => attrList
      case (uid,deg,None) => Array.empty[String]
    }

    val subgraph =graph.subgraph(vpred =(vid,attr) => attr.size ==2)

    //
    val pagerankGraph = subgraph.pageRank(0.001)

    //获取top pagerank用户的属性
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices){
      case (uid,attrList, Some(pr)) => (pr,attrList.toList)
      case (uid,attrList,None) => (0.0 ,attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))








  }

}
