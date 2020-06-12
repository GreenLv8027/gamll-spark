package com.atguigu.canal.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaUtil {
  private val prop = new Properties()
  prop.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadooop104:9092")
  prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String,String](prop)

  def send(topic: String, content: String): Unit ={
    producer.send(new ProducerRecord[String,String](topic, content))
  }

}
