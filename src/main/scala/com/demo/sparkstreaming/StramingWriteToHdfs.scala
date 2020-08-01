package com.demo.sparkstreaming

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.demo.sparkstreaming.rdd.RDDMultipleTextOutputFormat
import com.demo.utils.KafkaStreamingUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * created by chao.guo on 2020/8/1
 * // 读取kafka topic中的数据
 * 实时写入到hdfs 控制写出hdfs 的文件个数
 *
 *
 *
 **/
object StramingWriteToHdfs {
  var root_directory="/user/admin/sparkStreaming/"
  var  hdfs_master:String="hdfs://10.83.0.47:8020";
  var kuduMaster:String="node129:7051";
  var deployMode:String="local"
  var boker_list:String="10.83.0.47:9092";
  var topic:String ="maxwellBinlogData"
  var group_id:String="gc"
  var mode:String="test"

  def main(args: Array[String]): Unit = {
    // 处理请求参数
    if(mode!="test"){
      root_directory=args(0)
      hdfs_master =args(1)
      kuduMaster=args(2)
      deployMode=args(3)
      boker_list=args(4)
      topic=args(5)
      group_id=args(6)
      mode=args(7)
    }

    val sparkConf = new SparkConf().setAppName("StramingWriteToHdfs")
      .set("spark.streaming.backpressure.enabled", "true") // 开启被压
      .set("spark.streaming.kafka.maxRatePerPartition", "10") // 每秒 每个分区消费10条数据
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      if(deployMode=="local"){
        sparkConf.setMaster("local[*]")
      }
     // 优雅关闭
    System.setProperty("HADOOP_USER_NAME", "admin")
    val param: Map[String, String] = Map[String, String] (
      "boker_list" -> boker_list,
      "groupId" -> group_id,
      "topic" -> topic,
      "kudu_table_name" -> "impala::default.kafka_offsit",
      "isAutoCommit" -> "false",
      "type" -> "earliest",
      "kudu_master" -> kuduMaster
    )
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val kafkaDstream = KafkaStreamingUtils.getKafkaDstream(ssc , param)

    val mapDstream = kafkaDstream.mapPartitions(it => {
      var resList: List[(String, String)] = List[(String, String)]()
      while (it.hasNext) {
        try{// 解析json 字段中的数据库名和对应的表名
        val consumerRecord: ConsumerRecord[String, String] = it.next()
          val jsonData: fastjson.JSONObject = JSON.parseObject(consumerRecord.value())
        val dataBase = jsonData.get("database")
        val tableName = jsonData.get("table")
        resList = (dataBase + "_" + tableName, consumerRecord.value()) :: resList
        }catch {
          case e:Exception=>"json 格式异常"+e.printStackTrace()
        }
      }
      resList.iterator
    })
  // 将数据以追加的方式写入hdfs 目录根据 库名/表名
    mapDstream.foreachRDD(rdd=>{
      var conf: JobConf = new JobConf(rdd.context.hadoopConfiguration)
      conf.set("dfs.support.append","true")
      conf.set("fs.defaultFS", hdfs_master) //hdfs://10.83.0.47:8020
      rdd.saveAsHadoopFile(root_directory,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat],conf)


    })
    KafkaStreamingUtils.commitOffset(kafkaDstream,param)



    // 针对kafka 中的数据 进行分类 分成topic  value 类型 二元组
//    mapDstream.foreachRDD(rdd=>{
//        rdd.foreachPartition(it=>{
//          while (it.hasNext){
//            println(it.next()._1)
//            println(it.next()._2)
//          }
//        })
//
//      })
ssc.start()
    ssc.awaitTermination()









  }
}
