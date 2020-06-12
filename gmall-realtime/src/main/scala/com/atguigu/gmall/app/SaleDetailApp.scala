package com.atguigu.gmall.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.{ESUtil, MyKafkaUtil, RedisUtils}
import com.fasterxml.jackson.databind.ser.std.StdKeySerializers.Default
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

object SaleDetailApp {
  val url = "jdbc:mysql://hadoop102:3306/gmall1128"
  val props: Properties = new Properties()
  props.setProperty("user", "root")
  props.setProperty("password", "123")

  def CreateInfoAndDetailStream(ssc: StreamingContext) = {
    val orderInfoStream = MyKafkaUtil.getKafkaStream(ssc,Constant.ORDER_INFO_TOPIC)
      .map(ele => JSON.parseObject(ele, classOf[OrderInfo]))

    val orderDetailStream = MyKafkaUtil.getKafkaStream(ssc,Constant.ORDER_DETAIL_TOPIC)
      .map(ele => JSON.parseObject(ele, classOf[OrderDetail]))

    (orderInfoStream, orderDetailStream)
  }

  /**
    * 保存到Redis，使用set
    * @param str
    * @param orderInfo
    * @param client
    * @param timeout 过期时间，单位 秒
    * @return
    */
  def saveToRedis(key: String, value: AnyRef, client: Jedis, timeout: Int) = {

    val content = Serialization.write(value)(DefaultFormats)
    client.setex(key, timeout,content)
  }

  /**
    * 缓存orderInfo到redis，设置过期时间为30分钟
 *
    * @param orderInfo
    * @return
    */
  def cacheOrderInfo(orderInfo: OrderInfo, client: Jedis) = {
    saveToRedis("orderInfo:"+orderInfo.id, orderInfo, client, 30 * 60)
  }

  /**
    * 缓存order_detail
    * @param orderDetail
    * @param client
    * @return
    */
  def cacheOrderDetail(orderDetail: OrderDetail, client: Jedis) = {
    saveToRedis("order_detail:" + orderDetail.order_id + ":" + orderDetail.id, orderDetail, client, 30 * 60)
  }

  /**
    * 双流join，并返回合并后的流
    *
    * @param orderInfoStream
    * @param orderDetailStream
    */
  def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
    //join前，需要将数据格式变为kv
    val orderInfo = orderInfoStream.map(info => (info.id, info))
    val orderDetailInfo = orderDetailStream.map(info => (info.order_id, info))

    orderInfo.fullOuterJoin(orderDetailInfo)
      .mapPartitions(it => {
        val client = RedisUtils.getRedisClient
        val result = it.flatMap {
          case (orderId, (Some(orderInfo), Some(orderDetail))) => {
            //1.先将orderInfo存入redis中
            cacheOrderInfo(orderInfo, client)
            //2.和当前批次进行join
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
            //3.查找redis中缓存的orderDetail的id，key的格式为order_detail:id:*
            //获取所有的符合条件的key
            import scala.collection.JavaConversions._
            val cacheOrderDetailKeys = client.keys("order_detail:" + orderInfo.id + ":*").toList
            //redis返回的是Java集合，但需要用scala的集合算子
            val saleDetails = cacheOrderDetailKeys.map(key => {
              val cacheOrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
              client.del(key)
              SaleDetail().mergeOrderDetail(cacheOrderDetail).mergeOrderInfo(orderInfo)
            })
            saleDetails
          }
          case (orderId, (Some(orderInfo), None)) => {
            //1.先将orderInfo存入redis中
            cacheOrderInfo(orderInfo, client)
            //2.查找redis中缓存的orderDetail的id，key的格式为order_detail:id:*
            //获取所有的符合条件的key
            import scala.collection.JavaConversions._
            val cacheOrderDetailKeys = client.keys("order_detail:" + orderInfo.id + ":*").toList
            //redis返回的是Java集合，但需要用scala的集合算子
            val saleDetails = cacheOrderDetailKeys.map(key => {
              val cacheOrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
              client.del(key)
              SaleDetail().mergeOrderDetail(cacheOrderDetail).mergeOrderInfo(orderInfo)
            })
            saleDetails
          }
          case (orderId, (None, Some(orderDetail))) => {
            //1.查redis中，有没有缓存的order_info
            val cacheOrderInfo = JSON.parseObject(client.get("orderInfo:"+orderDetail.order_id), classOf[OrderInfo])
            if(cacheOrderInfo != null) {
              val saleDetail = SaleDetail().mergeOrderInfo(cacheOrderInfo).mergeOrderDetail(orderDetail)
              client.del("order_detail:" + orderDetail.order_id + ":" + orderDetail.id)
              saleDetail :: Nil
            }
              //否则，将order_detail存入缓存中，格式key "order_detail:" + order_id1 + ":" + id1
            else {
              cacheOrderDetail(orderDetail, client)
              Nil
            }
          }
        }
        //关闭redis客户端
        client.close()

        result

      })

  }

  /**
    * 读取user信息，先将redis的数据和spark流中的user_id比较，看是否可以全部从redis中获取，
    * 若可以从redis中全部获取，则直接获取，返回到spark流中。
    * 若无法从redis中全部获取，则查询mysql，存入redis，并返回到spark流中。
    * 返回值，是RDD形式的user信息
    * @param spark
    * @param strings
    * 其中，redis缓存user信息的格式如下，使用hash结构
    * key                             value
    * "user_info"                     hash
    *                                 field                   value
    *                                 user_id                 用户信息的json字符串
    */
  def readUserInfo(spark: SparkSession, userIds: Array[String]) = {
    val client = RedisUtils.getRedisClient
    import scala.collection.JavaConversions._

    //查看在redis缓存中，与spark流中的user_id，对应上的user_id，返回List
    val userIdAndUserInfoList = client.hgetAll("user_info").toList.filter {
      case (userId, userInfoString) => {
        userIds.contains(userId)
      }
    }
    //比较，redis缓存的userId列表，与spark流中userId列表，判断是否这两个列表数量相等，若相等则说明redis缓存中已经存储了本批次spark流中userId的信息
    if (userIdAndUserInfoList.length == userIds.length) {
      val userInfo = userIdAndUserInfoList.map {
        case (userId, userInfoString) => (userId, JSON.parseObject(userInfoString, classOf[UserInfo]))
      }
      //需要返回RDD，用spark创建
      spark.sparkContext.parallelize(userInfo)
    }
    //否则需要从mysql中读取
    else {
      //.as 需要1个隐式
      import spark.implicits._
      val userInfoRDD = spark.read
        .jdbc(url, "user_info", props)
        .as[UserInfo]
        .map(info => (info.id, info))
        .rdd

      //需要将user信息存入redis
      userInfoRDD.foreachPartition(it => {
        val client = RedisUtils.getRedisClient

        it.foreach{
          case (userId, userInfo) => {
            client.hset("user_info", userId, Serialization.write(userInfo)(DefaultFormats))
          }
        }
      client.close()
      })
      //返回RDD形式的userInfo
      userInfoRDD
    }

  }

  /**
    * 将user信息join到saleDetail流中
    *
    * @param saleDetailStream
    * @param sparkContext 用于创建spark session
    * @return
    */
  def joinUser(saleDetailStream: DStream[SaleDetail], sc: SparkContext) = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    saleDetailStream.transform(rdd => {
      //给rdd做缓存，因为后面readUserInfo中的collect会执行1次算子，导致redis中丢部分order_detail数据
      rdd.cache()

      //每个RDD中，读取user数据
      //将saleDetail流，增加user_id作为key，为后续的join做准备
      val saleDetailRDD = rdd.map(detail => (detail.user_id, detail))

      //读取user数据，将rdd中的user_id取出，并去重
      val userInfo = readUserInfo(spark, rdd.map(_.user_id).distinct().collect())

      //将user信息join到saleDetail流中
      saleDetailRDD
        .join(userInfo)
        .map{
          case (_, (saleDetail, userInfo)) => {
              saleDetail.mergeUserInfo(userInfo)
          }
        }
    })
  }


  /**
    * 将spark流信息存入到es
    * @param resultStream
    */
  def saveToES(resultStream: DStream[SaleDetail]) = {
    resultStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        ESUtil.insertBulk("gmall_sale_detail", it)
      })
    })

  }

  def main(args: Array[String]): Unit = {
    //1.获取2个流，并转为KV形式
    val conf = new SparkConf().setAppName("SaleDetail").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val (orderInfoStream, orderDetailStream) = CreateInfoAndDetailStream(ssc)

    //2.对2个流进行full outer join
    val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)

    //3.反查User信息
    val resultStream = joinUser(saleDetailStream, ssc.sparkContext)

    //存入es
    saveToES(resultStream)

    ssc.start()
    ssc.awaitTermination()
  }

}
