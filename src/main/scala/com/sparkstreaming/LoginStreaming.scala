package com.sparkstreaming

import com.alibaba.fastjson.JSON
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lichangyue on 2016/10/21.
 */
object LoginStreaming {
  val privateKey = "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAjbSzK8dEgcU2PztVyiIB9BjCDgwjpN4g3H15Uq2Dxw12hyKRZDXmRuXTjxaEqfAE4aVTzIoYM0aJLUrpHhwqAwIDAQABAkAgcbf4FQalY57Y+V/aCNFDrwt3JeZfUBBcC0pk2J9sSlwbQ++SN778dvK+yhZ8Q9jrDVVIo9V+iZSpHdqXjKzRAiEA0+iNkWy8CLY+PAsL3iEFU8W7LI9P8RRznXywqu1bNwsCIQCrMMKBGUBZdVL3P9oKAuoF+Nmdjrm3KPCWGZ8CBsvT6QIgRK5V2/FrDEPM7fcClK8NI/atUKbuWQuw4TU9qVievLsCIBY+AJeLc1vsLXpodmjklglumr+o4qJUlGW8MHev8F25AiBe3cgLML5cWgrDCGQhBJNtJalNw9ETzay3pGMRCXYc1Q=="

  case class Login(uid:String ,ct:String, name:String, type1:String,type2:String,ctdate :String)

  val groupId = "login_spark_consumer"
  val topic = "topic_another_device_id"
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
  val zkTopicPath = s"/kafka_spout/${topic}/spark_streaming" //存放offset的节点


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("LoginStreaming").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
   /* val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("hive.exec.max.dynamic.partitions.pernode", "100000")
    hiveContext.setConf("hive.exec.max.dynamic.partitions", "100000")*/



    // Kafka configurations
    /*val topics = Set("topic_another_device_id")
    val brokers = "10.58.188.206:9092,10.58.22.220:9092,10.58.22.221:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
*/
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
//        rdd.map(_._2).foreachPartition { element => element.foreach { println } }
        val events = rdd.flatMap(line => {
          if(line._2.startsWith("\"")){
            val str = line._2.replaceAll("\\\\","")
            println(">>>>>>>>>>>>>>>>>:"+str)
            val data = JSON.parseObject(str.substring(1,str.length -1))
            Some(data)
          }else{
            val data = JSON.parseObject(line._2)
            Some(data)
          }
        })
        //events.foreach(println)



        for (o <- offsetRanges) { //zk中的
          /**
           * {"topology":{"id":"7f9c6f90-bd8b-46f4-aeeb-399cc72fdcad","name":"mobile-access"},"offset":208629079,"partition":1,"broker":{"host":"S7SA058","port":9092},"topic":"mobile-access"}

           */
          var json = "{\"topology\":{\"id\":\""+this.getClass.getSimpleName+"\",\"name\":\""+ groupId +"\"},\"offset\":"+ o.fromOffset +
            ",\"partition\":"+o.partition+",\"broker\":{\"host\":\"10.58.22.220\",\"port\":9092},\"topic\":\"" + topic + "\"}"

          ZkUtils.updatePersistentPath(zkClient, s"${zkTopicPath}/partition_${o.partition}", json)
        }
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
 }
