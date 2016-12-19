package com.streaming

import java.util

import com.alibaba.fastjson.JSON
import com.streaming.utils.RiskDateUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext, Seconds}
import org.joda.time.DateTime
import org.json.JSONObject


/**
 * Created by lichangyue on 2016/10/17.
 */
object OrderStreaming {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    var masterUrl = "local[1]"
    /*if (args.length > 0) {
      masterUrl = args(0)
    }*/

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("test1")
//        val ssc = new StreamingContext(conf, Minutes(5))
    val ssc = new StreamingContext(conf, Seconds(30))
    // Kafka configurations
    val topics = Set("oms_order")
    val brokers = "sss:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")




    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      //val data = JSONObject.fromObject(line._2)
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    events.print()


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

    val orderRDD = events.map(x => {

      try{


        var profileId = x.getLong("profileId")
        var orderInfo = x.getJSONObject("orderInfo")
        if(orderInfo ==null){
          ""
        }else{
          var ipArr = orderInfo.getString("submittedIpAddress").split(",")
          var ip =""
          if(ipArr != null  && ipArr.size !=0 ){
            ip = ipArr(0)
          }

          var shippingGroups = x.getJSONArray("shippingGroups")
          if(shippingGroups.size() == 0){
            ""
          } else {
            var shippingGroup1 = shippingGroups.getJSONObject(0)
            var shippingGroupAddress = shippingGroup1.getJSONObject("shippingGroupAddress")

            var addressDetail = shippingGroupAddress.getString("cityCode") +
              shippingGroupAddress.getString("stateCode") +
              shippingGroupAddress.getString("countyCode") + shippingGroupAddress.getString("townCode")

            var email = shippingGroupAddress.getString("email")
            var mobileNumber = mobileParseUtil.evaluate(shippingGroupAddress.getString("mobileNumber"))
            var address = shippingGroupAddress.getString("address")

            //2016-10-21T10:27:13+08:00
            var creationDate = RiskDateUtil.formatDate(x.getString("creationDate"))

            var userAttrMap = Map(18->ip,15 ->addressDetail,12 ->mobileNumber,16 ->address )
            var buffer = new StringBuffer("")


            userAttrMap.foreach(name => {
              //userid, createtime ,name,type1,type2

              if(name._1 ==16 )
                buffer.append("11"+profileId +","+creationDate +","+ name._2+ ","+ name._1 +",trade")
              else
                buffer.append("11"+profileId +","+creationDate +","+ name._2+ ","+ name._1 +",trade\n")
            })
            buffer.toString
          }
        }

      }catch{
        case ex: Exception => ""
      }

    })

    orderRDD.filter{case x => x != null && x !=""}.saveAsTextFiles("hdfs://S1SA300:8020/streaming/" + RiskDateUtil.getTodayday + "/order")


    ssc.start()
    ssc.awaitTermination()
  }
}
