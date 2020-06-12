package com.atguigu.gmall.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.StartupLog
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 1. 消费kafka数据
    val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)

    //2. 流中的数据封装到样例类中
    val startupStream = sourceStream.map(log => JSON.parseObject(log, classOf[StartupLog]))

    //3. 借助redis去重
    val filterStartupStream = startupStream.transform(rdd => {
      val client = RedisUtils.getRedisClient
      //获取redis中的mid
      val mids: util.Set[String] = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      client.close()
      val broad = ssc.sparkContext.broadcast(mids)
     // println(broad.value.getClass.getName)
      rdd.filter(per => !broad.value.contains(per.mid))
        .map(log => (log.mid, log))
        .groupByKey()
        .map{
          //尝试使用TreeSet
          case (mid, log) => log.toList.minBy(_.ts)
        }

    })

    filterStartupStream.print()

    //3.1过滤后的mid，要添加到redis
    filterStartupStream.foreachRDD(rdd => {
      //为什么不直接用foreach，原因和将RDD数据存入mysql一样，不用建立多次连接，
      //只需要建立分区数这么多个的连接就可以
      rdd.foreachPartition(it => {
        val client = RedisUtils.getRedisClient
        it.foreach(log => {
          client.sadd(Constant.STARTUP_TOPIC + ":" + log.logDate, log.mid)
        })
        client.close()
      })
      // 4. 新启动的设备写入到hbase, 通过phoenix
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_DAU",
        // 这个地方写的是phoenix中的表的列名!!!
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")

      )
    })


/*    val client = RedisUtils.getRedisClient
    println(client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())))*/

    ssc.start()
    ssc.awaitTermination()
  }




}
