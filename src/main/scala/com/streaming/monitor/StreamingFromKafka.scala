package com.streaming.monitor

/**
 * Created by lichangyue on 2016/12/16.
 */

import com.alibaba.fastjson.JSON
import com.streaming.utils.{RiskDateUtil, MobileParseUtil, RSA}
import kafka.serializer.StringDecoder
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.log4j.{ Level, Logger }
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import kafka.utils.ZkUtils
import kafka.utils.ZKGroupTopicDirs
import org.apache.spark.streaming.dstream.InputDStream
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest




object StreamingFromKafka {
  val groupId = "order_spark_consumer"
  val topic = "oms_order"
  val zkClient = new ZkClient("10.58.188.206:2181,10.58.22.220:2181,10.58.22.221:2181", 60000, 60000, new ZkSerializer {
    override def serialize(data: Object): Array[Byte] = {
      try {
        return data.toString().getBytes("UTF-8")
      } catch {
        case e: ZkMarshallingError => return null

      }
    }
    override def deserialize(bytes: Array[Byte]): Object = {
      try {
        return new String(bytes, "UTF-8")
      } catch {
        case e: ZkMarshallingError => return null
      }
    }
  })


  /*val topicDirs = new ZKGroupTopicDirs("spark_streaming", topic)
  val zkTopicPath = s"${topicDirs.consumerOffsetDir}"//  zk上存储offset的地址， zkTopicPath = /consumers/spark_streaming/offsets/oms_order*/
  val zkTopicPath = s"/kafka_spout/${topic}/spark_streaming" //存放offset的节点

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.DEBUG)


    val sparkConf = new SparkConf().setAppName("orderstreaming")

    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

   /* val topics = Set("oms_order")
    val brokers = "10.58.188.206:9092,10.58.22.220:9092,10.58.22.221:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")*/

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))
    val kafkaParams = Map(
      "metadata.broker.list" -> "10.58.188.206:9092,10.58.22.220:9092,10.58.22.221:9092",
      "group.id" -> groupId,
      //"zookeeper.connect"->"10.58.188.206:2181,10.58.22.220:2181,10.58.22.221:2181",
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString )



    val topics = Set(topic)
    val children = zkClient.countChildren(zkTopicPath)
    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0) {
      //---get partition leader begin----
      val topicList = List(topic)
      val req = new TopicMetadataRequest(topicList,0)  //得到该topic的一些信息，比如broker,partition分布情况
      val getLeaderConsumer = new SimpleConsumer("10.58.188.206",9092,10000,10000,"OffsetLookup") // brokerList的host 、brokerList的port、过期时间、过期时间
      val res = getLeaderConsumer.send(req)  //TopicMetadataRequest   topic broker partition 的一些信息
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match{
        case Some(tm) =>
          tm.partitionsMetadata.map(pm=>(pm.partitionId,pm.leader.get.host)).toMap[Int,String]
        case None =>
          Map[Int,String]()
      }
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/partition_${i}")
        var json = JSON.parseObject(partitionOffset)
        var nextOffset = json.getLong("offset")

        val tp = TopicAndPartition(topic, i) //[topic,1]
        //---additional begin-----
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))  // -2,1
        val consumerMin = new SimpleConsumer(partitions(i),9092,10000,10000,"getMinOffset")
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        if(curOffsets.length >0 && nextOffset < curOffsets.head){  //如果下一个offset小于当前的offset
          nextOffset = curOffsets.head
        }
        //---additional end-----
        fromOffsets += (tp -> nextOffset) //[topic,1]-> offset
//        fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD {
      rdd =>
      {
        //rdd.map(_._2).foreachPartition { element => element.foreach { println } }


        for (o <- offsetRanges) { //zk中的
          /**
           * {"topology":{"id":"7f9c6f90-bd8b-46f4-aeeb-399cc72fdcad","name":"mobile-access"},"offset":208629079,"partition":1,"broker":{"host":"S7SA058","port":9092},"topic":"mobile-access"}

           */
          var json = "{\"topology\":{\"id\":\"\"+this.getClass.getSimpleName+\"\",\"name\":\""+ groupId +"\"},\"offset\":"+ o.fromOffset +
            ",\"partition\":"+o.partition+",\"broker\":{\"host\":\"10.58.22.220\",\"port\":9092},\"topic\":\"" + topic + "\"}"

          ZkUtils.updatePersistentPath(zkClient, s"${zkTopicPath}/partition_${o.partition}", json)
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
