package com.demo.utils

import java.{lang, util}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.{KuduClient, KuduPredicate, RowResultIterator}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * created by chao.guo on 2020/8/1
 **/
object KafkaStreamingUtils {


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
