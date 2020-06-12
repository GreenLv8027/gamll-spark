package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.common.utils.PropertyUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig

object MyKafkaUtil {

  private val params = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertyUtil.getProperty("config.properties", "kafka.servers"),
    ConsumerConfig.GROUP_ID_CONFIG -> PropertyUtil.getProperty("config.properties", "kafka.group.id")
  )

  def getKafkaStream(sc: StreamingContext, topic: String) = {

    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
      sc,
      params,
      Set(topic))
      .map(_._2)

  }


}
