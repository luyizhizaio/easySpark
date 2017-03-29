package com.analysis

import com.analysis.common.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
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

    //合并所有主题到大RDD中
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

    //最常出现的伴生二元组
    cooccurs.top(10)(Ordering.by[(Seq[String],Int),Int](_._2)).foreach(println)
//    (List(Analgesia, Anesthesia),181)
//    (List(Analgesia, Anesthesia and Analgesia),179)




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
