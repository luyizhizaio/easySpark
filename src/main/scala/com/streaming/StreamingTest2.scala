package com.streaming

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}


/**
 * Created by lichangyue on 2016/10/17.
 */
object StreamingTest2 {

  case class Point(id: Long, name: String, ptype: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    var masterUrl = "local[1]"
    /*if (args.length > 0) {
      masterUrl = args(0)
    }*/

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("test1")
    val ssc = new StreamingContext(conf, Minutes(5))

    // Kafka configurations
    val topics = Set("oms_order_test")
    val brokers = "10.58.22.219:9092,10.58.22.220:9092,10.58.22.221:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")


    val sc = ssc.sparkContext


    val sqlContext = new SQLContext(sc)

    val pointFile = sc.textFile("file/data/streaming/a.txt")

    val pointRDD = pointFile.map { x => x.split(" ") }.map { data =>
    {
      Point(data(0).trim().toLong, data(1).trim(), data(2).trim)
    }
    }

    //
    import sqlContext.implicits._
    val df = pointRDD.toDF()
    df.registerTempTable("point")

    /**
     *
     {
    "id": "81ac3c28d979abfb6fbac5a0c8cce9908b2994d7f62916833520b0486db9d7ebafe59d2a7bc5e882d777d12cb34794e1d2ba75956d8c9f7d5c38db462528b838",
    "eml": "",
    "rt": "2016-10-17 17:32:53",
    "ph": "13662545004",
    "un": "liuyilucia",
    "ip": "125.88.24.60",
    "fromType": 3
     correct	1(成功)/0(失败)	是否登录成功/注册
platform	android/IOS	设备类型
version	只发大版本	app版本号

}
[uuid=c92dc259-8c12-4921-b273-e68fc9f0fd30,
    id=7acc605a26c527785eb5d2eb7d6e8aaf,
    uid=null,
    un=liuyilucia,
    ph=13662545004,
    eml=, ip=125.88.24.60,
    rt=2016-10-17 17:32:53,
    ct=2016-10-17 17:32:54.928,
    up=2016-10-17 17:32:54.928,
    fromType=3]
      */

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      //val data = JSONObject.fromObject(line._2)
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    events.print()

    val count = events.count();

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
/*
    {
    "orderInfo": {
        "ufpd": "03afb3f38d82374b987b78480489a92b1a1d9e2c107800192d0eb1e8e647307156acc035c733c9bb421976f83f05425ab9a6fc235f54d4f02fa2beea3d1daf4d",
        "salesChannel": 0,
        "submittedIpAddress": "10.126.53.115,0",
        "orderId": "15102664895",
        "origOrderCpscoupon": 0
    },
    "creationDate": "2016-10-20T15:54:27+08:00",
    "profileId": "100014382456",
    "industryCheckFlag": false,
    "shippingCost": 5,
    "rawSubtotal": 11,
    "shippingGroups": [
        {
            "finalAmount": 11,
            "shippingGroupAddress": {
                "shippingGroupId": "2568888279",
                "totalAddress": "nullnullnullnull前方一转弯垃圾桶旁边的胡同",
                "address": "前方一转弯垃圾桶旁边的胡同",
                "name": "隔壁老李",
                "cityCode": "11010000",
                "stateCode": "11000000",
                "mobileNumber": "1899129815435007",
                "countyCode": "11010200",
                "country": "1",
                "townCode": "11010200"
            },
            "shippingGroupId": "2568888279",
            "shipAmount": 5,
            "orderId": "15102664895"
        }
    ],
    "orderId": "15102664895"
}
    */

    val orderRDD = events.map(x =>{
//      var orderId = x.getLong("orderId")
      var profileId = x.getLong("profileId")

      var orderInfo = x.getJSONObject("orderInfo")
      var ip = orderInfo.getString("submittedIpAddress").split(",")(0)
      //2016-10-21T10:27:13+08:00
      var creationDate = x.getString("creationDate")

      var shippingGroups = x.getJSONArray("shippingGroups")
      var shippingGroup1 = shippingGroups.getJSONObject(0)
      var shippingGroupAddress = shippingGroup1.getJSONObject("shippingGroupAddress")

      var totalAddress = shippingGroupAddress.getString("totalAddress")
      var email = shippingGroupAddress.getString("email")
      var mobileNumber = shippingGroupAddress.getString("mobileNumber")
      var address = shippingGroupAddress.getString("address")
      
      (profileId,ip,totalAddress,email,mobileNumber,address,creationDate )

    })

    orderRDD.foreachRDD{rdd =>

      rdd.foreach(t => {
        //(profileId,ip,totalAddress,email,mobileNumber,address,creationDate )



          sqlContext.sql("select age from people where age>=25").show()



      })


      rdd.foreach( x => print(x + ","))
      println("\n\n")
    /*val wordCountsDataFrame = rdd.toDF()
    wordCountsDataFrame.registerTempTable("word_counts")*/

    }
    // Compute user click times
    /*val userClicks = events.map(x => (x.getString("uid"), x.getInteger("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          RedisClient.pool.returnResource(jedis)
        })
      })
    })*/

    ssc.start()
    ssc.awaitTermination()

  }
}
