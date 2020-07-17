package com.demo.gc

import java.{lang, util}

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.{ KuduClient, KuduPredicate, RowResultIterator}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * created by chao.guo on 2020/4/13
 **/
object KafkaTest {
  //指定从latest(最新,其他版本的是largest这里不行)earliest(最早)处开始读取数据

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
      .set("spark.streaming.backpressure.enabled", "true")  // 开启被压
      .set("spark.streaming.kafka.maxRatePerPartition", "5") // 每秒 每个分区消费10条数据
      .set("spark.streaming.stopGracefullyOnShutdown", "true"); // 优雅关闭
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3)) // 3 秒一个批次
    val param: Map[String, String] = Map[String, String] (
      "boker_list" -> "10.83.0.47:9092",
      "groupId" -> "gc_test",
      "topic" -> "maxwells",
      "kudu_table_name" -> "impala::default.kafka_offsit",
      "isAutoCommit" -> "false",
      "type" -> "earliest",
      "kudu_master" -> "node129:7051"
    )
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = getKafkaDstream(ssc,param)
    // 转换数据结构
    val valueStream: DStream[(String, String)] = kafkaDstream.mapPartitions(it => {
      var strings = List[(String,String)]()
      while (it.hasNext) {
        val consumer: ConsumerRecord[String, String] = it.next()
       val data = consumer.value()
        val jsonData: fastjson.JSONObject = JSON.parseObject(data)
        val deal_date = jsonData.getJSONObject("data").getString("DEAL_DATE")
        strings = (deal_date,data)::strings
      }
      strings.iterator
    })
    //按照key 做为文件夹的名称 写出数据
    // key 为对应的时间  /user/admin/test00000/2020-02-06/part-00000
    // key value  类型的rdd  写出去的时候 com.demo.gc.AppendTextOutputFormat.MyLineRecordWriter.writeObject 这里面 注释掉写key的代码
    valueStream.foreachRDD(rdd=>{
      rdd.saveAsHadoopFile("/user/admin/test00000/",classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])

    })
  //提交offset
    Thread.sleep(1000)
    commitOffset(kafkaDstream,param)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 查询对应的kudu 表
   *  create table
   *
   * CREATE TABLE default.kafka_offsit(
   * id bigint comment 'id',
   * groupid String  comment '消費者組',
   * topic String comment '主題',
   * topic_partition int comment '分区号',
   * untiloffset STRING comment '偏移量',
   * PRIMARY KEY(id)
   * )
   * PARTITION BY HASH PARTITIONS 6
   * comment 'kafka偏移量表'
   * STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');
   *
   *
   *
   * //val sql="select `groupid`,`topic`,`partition`,`untiloffset` from offset_manager where groupid=? and topic=?"
   *
   * @param param
   * @return
   */
  def getOffsetFromKudu(param:Map[String,String]): util.Map[TopicPartition, lang.Long] = {
    val kudu_client = new KuduClient.KuduClientBuilder(param("kudu_master")).build()
    val clumns: util.ArrayList[String] = new util.ArrayList[String]()
    clumns.add("groupid")
    clumns.add("topic")
    clumns.add("topic_partition")
    clumns.add("untiloffset")
    val kuduTable = kudu_client.openTable(param("kudu_table_name"))
    val kuduTableScaner = kudu_client.newScannerBuilder(kuduTable).setProjectedColumnNames(clumns)
    val predicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema.getColumn("groupid"),KuduPredicate.ComparisonOp.EQUAL,param("groupId"))
    val predicate2 = KuduPredicate.newComparisonPredicate(kuduTable.getSchema.getColumn("topic"),KuduPredicate.ComparisonOp.EQUAL,param("topic"))
    kuduTableScaner.addPredicate(predicate).addPredicate(predicate2);
    val scanner = kuduTableScaner.build()
    val resMap: util.Map[TopicPartition, java.lang.Long] = new util.HashMap[TopicPartition,java.lang.Long]()
    while (scanner.hasMoreRows) {
      val row: RowResultIterator = scanner.nextRows()
       while (row.hasNext) {
        val line = row.next()
         val topicPartition: TopicPartition = new  TopicPartition(line.getString("topic"),line.getInt("topic_partition"))
         resMap .put(topicPartition,line.getString("untiloffset").toLong)
      }
    }
    kudu_client.close()
    resMap
  }

  /**
   * 创建kafka 的dstream
   * @param ssc
   * @param param
   * @return
   */
  def getKafkaDstream(ssc: StreamingContext, param:Map[String,String]): InputDStream[ConsumerRecord[String, String]] ={
    import  scala.collection.JavaConversions._
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> param("boker_list"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> param("groupId"),
      "auto.offset.reset" -> param("type"), //earliest
      "enable.auto.commit" -> param("isAutoCommit")//isAutoCommit
    )

    var fromOffsets: util.Map[TopicPartition, lang.Long] = new util.HashMap[TopicPartition, lang.Long]()
//    var fromOffsets: util.Map[TopicPartition, java.lang.Long] =new util.HashMap[TopicPartition, java.lang.Long]();
    val topics = Seq(param("topic")) //topic
    val isAutoCommit = param("isAutoCommit")
    if("false".equalsIgnoreCase(isAutoCommit)){
      fromOffsets = getOffsetFromKudu(param);
    }
    import org.apache.spark.streaming.kafka010._
    if(fromOffsets==null || fromOffsets.isEmpty  ){
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }else{
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, fromOffsets)
      )
    }
  }

  def commitOffset(kafkaDataDstream:InputDStream[ConsumerRecord[String, String]],param:Map[String,String]): Unit ={
    kafkaDataDstream.foreachRDD(rdd=>{
      //获取kudu的表连接
    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val kudu_client = new KuduClient.KuduClientBuilder(param("kudu_master")).build()
      val kuduTable = kudu_client.openTable(param("kudu_table_name"))
      val kuduSession = kudu_client.newSession
      import org.apache.kudu.client.SessionConfiguration
      kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)
      kuduSession.setMutationBufferSpace(3000)
      for(offset<-offsetRanges){
        val upsert = kuduTable.newUpsert()
        upsert.getRow.addLong("id",(param("groupId")+param("topic")).hashCode+offset.partition)
        upsert.getRow.addString("groupid",param("groupId"))
        upsert.getRow.addString("topic",param("topic"))
        upsert.getRow.addInt("topic_partition",offset.partition)
        upsert.getRow.addString("untiloffset",offset.untilOffset.toString)
        kuduSession.flush
        kuduSession.apply(upsert)
      }
      kuduSession.close()
    })

  }


}
