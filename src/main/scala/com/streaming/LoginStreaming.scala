package com.streaming

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Created by lichangyue on 2016/10/21.
 */
object LoginStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster("local[1]").setAppName("LoginStreaming")
//    val ssc = new StreamingContext(conf, Minutes(5))
    val ssc = new StreamingContext(conf, Seconds(5))
    // Kafka configurations
    val topics = Set("topic_another_device_id")
    val brokers = "sxx.22.219:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      //val data = JSONObject.fromObject(line._2)
      if(line._2.startsWith("\"")){
        val str = line._2.replaceAll("\\\\","")
        println(">>>>str>>>> :"+str)
        val data = JSON.parseObject(str.substring(1,str.length -1))
        Some(data)
      }else{
        val data = JSON.parseObject(line._2)
        Some(data)
      }
    })
    events.print()


    //,ID,UID,UN,PH,EML,IP,LT,CT,UP,FROMTYPE
    val loginRDD = events.map(x =>{
      //解析ufpd
      var ufpd = x.getString("id")
      var uid = x.getString("uid")
      var phone = x.getString("ph")
      var date = x.getString("lt")
      var ip = x.getString("ip")
      var username = x.getString("un")
      var fromType = x.getString("fromType")



      //userid, createtime ,name,type1,type2

      if(uid == null ||uid.trim ==""){
        ""
      }else{
        /**
        --账户-电话 11-12
--账户-邮箱 11-13
--账户-姓名 11-14
--账户-详细地址 11-15
--账户-标准地址 11-16
--账户-设备指纹 11-17
--账户-ip地址 11-18
--账户-证件 11-19
--账户-sku 11-22
          */


        var userAttrMap = Map(18->ip,12->phone,14 ->username,17->ufpd )
        var buffer = new StringBuffer("")
        userAttrMap.foreach(name => {
          //userid, createtime ,name,type1,type2

          if(name._1 ==17 )
            buffer.append("11"+uid +","+date +","+ name._2+ ","+ name._1 +",login")
          else
            buffer.append("11"+uid +","+date +","+ name._2+ ","+ name._1 +",login\n")
        })
        buffer.toString
      }})

    loginRDD.filter(x => x != null && x !="").saveAsTextFiles("hdfs://S1SA300:8020/streaming/" + RiskDateUtil.getTodayday + "/login")




    ssc.start()
    ssc.awaitTermination()

  }
 }
