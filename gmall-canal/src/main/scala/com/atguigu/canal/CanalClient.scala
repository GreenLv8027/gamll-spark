package com.atguigu.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{ CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.atguigu.canal.utils.MyKafkaUtil
import com.atguigu.gmall.common.Constant
import scala.collection.JavaConversions._

object CanalClient {

  def parseDate(rowList: util.List[CanalEntry.RowData],
                tableName: String,
                eventType: CanalEntry.EventType) = {
    //判断是否是目标表，event状态，rowData是否有数据
    if (tableName == "order_info" && eventType == EventType.INSERT && rowList != null &&
    rowList.size() > 0) {
      sentToKafka(Constant.ORDER_INFO_TOPIC,rowList)
    }else if(tableName == "order_detail" && eventType == EventType.INSERT && rowList != null &&
      rowList.size() > 0) {
      sentToKafka(Constant.ORDER_DETAIL_TOPIC,rowList)
    }
  }

  private def sentToKafka(topic: String, rowList: util.List[CanalEntry.RowData]) = {
    for (rowData <- rowList) {
      val json = new JSONObject()
      val columnsList = rowData.getAfterColumnsList
      for (column <- columnsList) {
        val key = column.getName
        val value = column.getValue
        json.put(key, value)
      }
      //发送数据，到kafka
      MyKafkaUtil.send(topic, json.toJSONString)
    }
  }

  def main(args: Array[String]): Unit = {
    //1.连接canal服务器
    val address = new InetSocketAddress("hadoop102",11111)
    val connector = CanalConnectors.newSingleConnector(address, "example", "", "")
    connector.connect()
    //2.订阅要处理的表
    connector.subscribe("gmall1128.*")
    //3.获取数据解析
    while(true){
      val message = connector.get(50)
      val entries = message.getEntries
      if (entries != null && entries.size() > 0) {
        import scala.collection.JavaConversions._
        for (entry <- entries) {
          if (entry !=null && entry.hasEntryType && entry.getEntryType==EntryType.ROWDATA) {
            val value = entry.getStoreValue
            val rowChange = RowChange.parseFrom(value)
            val rowList = rowChange.getRowDatasList
            //传递行数据，表名，EventType字段类型（存疑）
            parseDate(rowList, entry.getHeader.getTableName, rowChange.getEventType)
          }
        }
      } else{
        Thread.sleep(3000)
        println("3秒后再拉取数据")
      }
    }
  }

}
