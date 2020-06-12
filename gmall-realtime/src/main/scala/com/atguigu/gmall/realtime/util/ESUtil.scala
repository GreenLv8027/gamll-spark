package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

object ESUtil {

  val uri = "http://hadoop102:9200"
  private val httpConfig: HttpClientConfig = new HttpClientConfig.Builder(uri)
    .maxTotalConnection(100)
    .connTimeout(10000)
    .readTimeout(10000)
    .multiThreaded(true)
    .build()
  private val factory = new JestClientFactory()
  factory.setHttpClientConfig(httpConfig)

  def main(args: Array[String]): Unit = {
    //val source = User("xiaohuang", 25)
    val index = "user2"

    val source = Iterator(("10",User("hei", 22)),("11",User("bai", 333)))
    //insertSingle(index, source)
    insertBulk(index,source)
  }

  def simpleDemo() = {
    //1.通过工厂类创建客户端
    val factory = new JestClientFactory()
    //http客户端配置
    val config = new HttpClientConfig.Builder(uri)
      .maxTotalConnection(100)
      .connTimeout(10000)
      .readTimeout(10000)
      .multiThreaded(true)
      .build()

    factory.setHttpClientConfig(config)
    val client = factory.getObject

    //2.只写插入操作
    val source =
      """
        |{
        |  "name": "zhangsan",
        |  "age":20
        |}
      """.stripMargin


    val action = new Index.Builder(source)
        .index("user")
        .`type`("_doc")
      //id，为null时，会生成随机值
        .id("1")
        .build()
    client.execute(action)

  }

  /**
    * es插入单条数据
    * @param index
    * @param source
    * @param id ，可以不用赋值，null值则id随机
    */
  def insertSingle(index: String, source: Object, id: String=null) ={
      val client = factory.getObject
    val action = new Index.Builder(source)
      .index(index)
      //type统一为_doc
      .`type`("_doc")
      .id(id)
      .build()
    client.execute(action)
    client.shutdownClient()
  }


  /**
    * es插入多条数据，使用bulk
    * @param index
    * @param sources
    */
  def insertBulk(index: String, sources: Iterator[Object]) = {
    val client: JestClient = factory.getObject
    //创建1个Bulk桶，用于装入所有要添加的action
    //此时不能build，需要装入action之后再build
    val bulk = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")
    //sources的内容，可能是带有id的元组(id, data)，也可能是不带id的data
    sources.foreach{
      case (id: String, data) => {
        val action = new Index.Builder(data)
          .id(id)
          .build()
        //装入action
        bulk.addAction(action)
      }
      case data => {
        val action = new Index.Builder(data)
          .build()
        bulk.addAction(action)
      }
    }
    client.execute(bulk.build())
    client.shutdownClient()
  }

  case class User(name: String , age: Int)

  implicit class RichRDD(rdd: RDD[AlertInfo]){
    def saveToES(index: String): Unit ={
      rdd.foreachPartition(it => {
        //处理，增加ID
        val source = it.map(info => (info.mid + "_" + info.ts / 1000 / 60, info))
        ESUtil.insertBulk(index, source)
      })
    }
  }


}
