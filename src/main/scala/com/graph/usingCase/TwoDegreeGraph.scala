package com.graph.usingCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lichangyue on 2016/10/12.
 */
object TwoDegreeGraph {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("riskTest").setMaster("local")
      .set("spark.network.timeout", "1000s").set("spark.executor.heartbeatInterval", "1000s")
    val sc = new SparkContext(conf)


    val edge=List(//边的信息
      (1,2),(1,3),(2,3),(3,4),(3,5),(3,6),
      (4,5),(5,6),(7,8),(7,9),(8,9))


    //构建边的rdd
    val edgeRdd = sc.parallelize(edge).map(x =>{
      Edge(x._1.toLong , x._2.toLong, None)
    })
    //通过边构建图
    val graph = Graph.fromEdges(edgeRdd,0)

    //打印度,第一列表示顶点id，第二列表示各个顶点的度
    graph.degrees.collect.foreach(println(_))
//    (4,2)
//    (1,2)
//    (6,2)
//    (3,5)
//    (7,2)
//    (9,2)
//    (8,2)
//    (5,3)
//    (2,2)

    //二跳邻居
    /*使用两次遍历，首先进行初始化的时候将自己的生命值设为2，
     第一次遍历向邻居节点传播自身带的ID以及生命值为1(2-1)的消息，
     第二次遍历的时候收到消息的邻居再转发一次，生命值为0 */



    type VMap=Map[VertexId,Int]

    /**
     * 节点数据的更新 就是集合的union
     */
    def vprog(vid:VertexId,vdata:VMap,message:VMap)
    :Map[VertexId,Int]=addMaps(vdata,message)

    /**
     * 发送消息
     */
    def sendMsg(e:EdgeTriplet[VMap, _])={

      //取两个集合的差集  然后将生命值减1
      val srcMap=(e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k->(e.dstAttr(k)-1) }.toMap
      val dstMap=(e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k->(e.srcAttr(k)-1) }.toMap

      if(srcMap.size==0 && dstMap.size==0)
        Iterator.empty
      else
        Iterator((e.dstId,dstMap),(e.srcId,srcMap))
    }

    /**
     * 消息的合并
     */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map {
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
      }.toMap

    val two=2  //这里是二跳邻居 所以只需要定义为2即可
    val newG=graph.mapVertices((vid,_)=>Map[VertexId,Int](vid->two))
        .pregel(Map[VertexId,Int](), two, EdgeDirection.In)(vprog, sendMsg, addMaps)


    newG.vertices.collect().foreach(println(_))
    //(4,Map(5 -> 1, 1 -> 0, 6 -> 0, 2 -> 0, 3 -> 1, 4 -> 2))



    //进行顶点ID,和对应属性id的判断  转成 类型，数量
    val newG2  = newG.mapVertices((_,attr) =>{
      attr.size
    })


    newG2.vertices.collect().foreach(println(_))

  }

}
