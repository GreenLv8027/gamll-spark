package com.atguigu.gmall.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.bean.OrderInfo
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.phoenix.jdbc.PhoenixConnection
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
  val ssc = new StreamingContext(conf, Seconds(3))
  def main(args: Array[String]): Unit = {

    //1.取得kafka消费数据
    val sparkStream = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)

    //2.对数据进行脱敏，脱敏操作在Bean中已经进行，转成JSON字符串
    val orderInfo = sparkStream.map(ele => JSON.parseObject(ele, classOf[OrderInfo]))

    orderInfo.print()
    //3.通过phoenix写入HBase
    import org.apache.phoenix.spark._
    orderInfo.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
