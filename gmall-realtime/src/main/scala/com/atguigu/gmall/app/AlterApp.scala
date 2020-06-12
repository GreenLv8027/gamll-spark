package com.atguigu.gmall.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.{AlertInfo, EventLog}
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.{ESUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks

object AlterApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("alter")
    val ssc = new StreamingContext(conf, Seconds(5))
    //1.获取kafka信息
    val source = MyKafkaUtil.getKafkaStream(ssc, Constant.EVENT_TOPIC)

    //2.封装入样例类
    val beanStream = source.map(ele => JSON.parseObject(ele, classOf[EventLog]))

    //3.创建窗口
    val eventStream = beanStream.window(Minutes(5), Seconds(5))
      //4.取同1设备，为key，并做聚合
      .map(log => (log.mid, log))
      .groupByKey()

    //5.针对每个设备，增加3个集合信息，Set保存帐号，List保存行为，Set保存领取优惠券的商品ID信息（预留）
    //需要注意，因为后续要对接ES，所以集合都需要用java的集合
    val alterInfoStream = eventStream.map { case (mid, logIt) =>
      // 5分钟内的帐号
      val uidSet: util.HashSet[String] = new java.util.HashSet[String]()
      // 存储5分钟内所有的事件类型
      val eventList: util.ArrayList[String] = new java.util.ArrayList[String]()
      // 存储领取优惠券的那些商品id
      val itemSet: util.HashSet[String] = new java.util.HashSet[String]()
      // 是否浏览过商品. 默认没有
      var isClickItem = false
      Breaks.breakable(logIt.foreach(event => {
        eventList.add(event.eventId)
        event.eventId match {
          case "coupon" =>
            uidSet.add(event.uid)
            itemSet.add(event.itemId)
          case "clickItem" =>
            isClickItem = true
            Breaks.break
          case _ =>
        }
      })
      )
      //重要的，判断是否这个eventLog，需要被记录在该mid的“5分钟内的帐号的Set集合中”
      //如果第一个参数为true，则表示是需要产生警报的mid
      (!isClickItem && uidSet.size() >= 3, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    alterInfoStream.print()
    //处理因窗口滑动，造成的1分钟内，同一条mid信息被多次警报
    //使用es同ID覆盖的原理，将ID设置为“分钟数+mid”
    alterInfoStream
        .filter(_._1) //过滤出需要产生警报的信息
        .map(_._2) //只需要警报信息
        .foreachRDD(rdd => {
      /*rdd.foreachPartition(it => {
        //处理，增加ID
        val source = it.map(info => (info.mid + "_" + info.ts / 1000 / 60, info))
        ESUtil.insertBulk("alter", source)
      })*/
      import ESUtil._
      rdd.saveToES("alter")
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
