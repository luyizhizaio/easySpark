package com.analysis

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.analysis.common.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.graphx.{VertexRDD, VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.{XML, Elem}

/**
 * Created by lichangyue on 2017/3/29.
 */
object RunGraphTest {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Graph").setMaster("local[2]"))

    //加载xml原始文件,每一个行代表一个完整的xml字符串
    val medlineRaw = loadMedline(sc,"data/analy/medline16n0033.xml")
//    medlineRaw.take(1).foreach(println)

    //解析成xml对象, Elem代表一个xml对象
    val  mxml:RDD[Elem] = medlineRaw.map(XML.loadString)
    // 主题的集合
    val medline:RDD[Seq[String]] = mxml.map(majorTopics).cache()

    //合并所有主题
    val topics :RDD[String] = medline.flatMap(mesh => mesh)

    //计算各主题数量
    val topicCounts = topics.countByValue()
//    (Odontogenic Tumors,1)
//    (Spinal Cord Diseases,21)
//    (Crystallography, X-Ray,1)
    topicCounts.take(10).foreach(println)

    val tcSeq = topicCounts.toSeq
    //根据数量降序排列
    tcSeq.sortBy(_._2).reverse.take(10).foreach(println)

    //出现次数相同的主题的个数进行统计
    //groupBy根据第二个元素分组，得到结果： (69,Map(Abdominal Cavity -> 69, Pneumonia -> 69, Virus Diseases -> 69))
    // mapValues只修改value，key不变。
    val valueDist = topicCounts.groupBy(_._2).mapValues(_.size) //返回key：主题数量，value：相同数量主题的个数
    valueDist.toSeq.sorted.take(10).foreach(println)

    //combinations产生集合中两两组合的List,在调用combinations 之前要确保列表是排好序的
    val topicPairs = medline.flatMap(t => t.sorted.combinations(2))

    topicPairs.take(10).foreach(println)

    //计算主题对的数量
    val cooccurs = topicPairs.map(p => (p,1)).reduceByKey(_+_)

    cooccurs.cache()
    cooccurs.count()
    cooccurs.take(10).foreach(println)

    //最常出现的伴生二元组 ，Ordering指定排序字段
    cooccurs.top(10)(Ordering.by[(Seq[String],Int),Int](_._2)).foreach(println)
//    (List(Analgesia, Anesthesia),181)
//    (List(Analgesia, Anesthesia and Analgesia),179)


    val vertices = topics.map(topic => (hashId(topic),topic))

    val edges = cooccurs.map(p => {
      val (topics, cnt) = p  //主题对，出现的次数
      val ids = topics.map(hashId).sorted //生成边的id
      Edge(ids(0),ids(1),cnt)
    })

    val topicGraph = Graph(vertices,edges)
    topicGraph.cache()

    //计算连通图
    val connectedComponentGraph = topicGraph.connectedComponents()
    val componentCounts =sortedConnectedComponents(connectedComponentGraph)

    println(componentCounts.size)
    componentCounts.take(10).foreach(println)

    //主题名称和连通图的ID关联上 , (id,(name,componentId))
    val nameCID = topicGraph.vertices.innerJoin(connectedComponentGraph.vertices){
      (topicId,name,componentId) => (name,componentId)
    }

    //取出最大的连通图
    val c1 = nameCID.filter(x => x._2._2 == componentCounts(1)._1)
    //输出最大联通图中的主题
    c1.collect.foreach(x =>println(x._2._1))


    //统计包含HIV的主题，并统计数量
    val hiv = topics.filter(_.contains("HIV")).countByValue
    hiv.foreach(println)
    //(HIV Infections,1)

    //计算度
    val degrees:VertexRDD[Int] = topicGraph.degrees.cache()
    //计算度的统计量
    println(degrees.map(_._2).stats())
    //(count: 7596, mean: 19.104792, stdev: 40.490526, max: 1482.000000, min: 1.000000)

    //查找那些度最高的顶点的概念名称
    topNamesAndDegrees(degrees,topicGraph).foreach(println)
//    (Disease,1482)
//    (Neoplasms,918)

    //使用皮尔逊卡方测试检查主题的独立性。
    //卡方统计量大则表明随机变量相互独立的可能性小，因此两个概念同时出现是有意义的。

    val T = medline.count() //论文的数量
    //计算主题的数量
    val topicCountRdd = topics.map(x =>(hashId(x),1)).reduceByKey(_+_)







    sc.stop()

  }

  def topNamesAndDegrees(degrees:VertexRDD[Int],topicGraph:Graph[String,Int])
  : Array[(String,Int)] ={
    //每个主题的度  ,(id, (name,degree))
    val namesAndDegrees = degrees.innerJoin(topicGraph.vertices){
      (topicId,degree,name ) => (name,degree)
    }
    //定义排序
    val ord = Ordering.by[(String,Int),Int](_._2)
    namesAndDegrees.map(_._2).top(10)(ord)  //根据degree排序
  }


  /**
   * 统计联通图中，子联通图的节点数量
   * @param connectedComponents
   * @return
   */
  def sortedConnectedComponents(connectedComponents:Graph[VertexId,_]):Seq[(VertexId,Long)]={

    //求各连通图中节点的数量
    val componentCounts = connectedComponents.vertices.map(_._2).countByValue()
    componentCounts.toSeq.sortBy(_._2).reverse  //排序
  }

  /**
   * 使用hashCode生成ID
   * @param str
   * @return
   */
  def hashId(str: String): Long = {
    // This is effectively the same implementation as in Guava's Hashing, but 'inlined'
    // to avoid a dependency on Guava just for this. It creates a long from the first 8 bytes
    // of the (16 byte) MD5 hash, with first byte as least-significant byte in the long.
    val bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }


  /**
   * 读入xml文件
   * @param sc
   * @param path
   * @return
   */
  def loadMedline(sc:SparkContext, path:String):RDD[String]={
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY , "<MedlineCitation ")
    conf.set(XmlInputFormat.END_TAG_KEY , "</MedlineCitation>")

    val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
    classOf[LongWritable],classOf[Text],conf)
    in.map(line => line._2.toString)
  }

  def majorTopics(elem : Elem):Seq[String] ={
    //取出DescriptorName子节点集合 ,<DescriptorName MajorTopicYN="N" UI="D001519">Behavior</DescriptorName>
    val dn = elem \\ "DescriptorName"
    //取出MajorTopicYN属性值为Y的DescriptorName集合
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    //返回DescriptorName 标签的内容
    mt.map(n =>n.text)
  }


}
