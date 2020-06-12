package com.atguigu.gmall.realtime.util

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.gmall.common.Constant
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtils {
  val host = "192.168.1.102"
  val port = 6379

  val conf: JedisPoolConfig = new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(40)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true) // 忙碌的时候是否等待
  conf.setMaxWaitMillis(1000 * 60) // 最大等待时间
  conf.setTestOnBorrow(true) // 取客户端的时候, 是否做测试
  conf.setTestOnReturn(true)
  conf.setTestOnCreate(true)
  val pool = new JedisPool(conf, host, port)

  def getRedisClient = pool.getResource

  def main(args: Array[String]): Unit = {
    val client = RedisUtils.getRedisClient
    //获取redis中的mid
    //val mids = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
    val v = client.smembers("kk")
    val bool = Nil.contains("3")
    println(v)
    println(bool)
  }



}
