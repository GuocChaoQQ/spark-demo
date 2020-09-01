package com.demo.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Dataset, SparkSession}


/**
 * created by chao.guo on 2020/8/26
 **/
object DataWatch {
  def main(args: Array[String]): Unit = {
    // 读取规则表
    // 执行需要监控指标和监控
    // 执行sql
    // 将结果插入到mysql中
    //
    val spark = SparkSession.builder().
//      config("spark.sql.warehouse.dir", "hdfs://10.83.0.47:8020/user/hive/warehouse")
//      .config("hive.metastore.uris","thrift://10.83.0.47:9083")
      config("hive.execution.engine", "mr")
      //.master("local[*]")
      .appName("DataWatch")
      .enableHiveSupport()
      .getOrCreate()
    //SendEmailUtil.initEmailSession(spark,"file:\\E:\\drawData\\email.properties")
    if(args.size<3){
      System.err.println("请传入 邮件配置地址")
      System.exit(1);

    }
   SendEmailUtil.initEmailSession(spark,args(0))
    val batch_date =args(1)
    val last_batch_date = clcalLastBatchDate(batch_date)





    val sqlSet = spark.read.format("kudu").option("kudu.master",args(2))
      .option("kudu.table","impala::ods.data_watch")
      .load().coalesce(1)

  val array= sqlSet.collect().map(it => {
      var tuplesToTuples = Map[String, String]()
      val schemas = it.schema
      schemas.foreach(schema => {
        var value=""
        if("id"==schema.name){
          value = it.getAs[Integer](schema.name).toString
        }else{
          value =it.getAs[String](schema.name)
        }
        tuplesToTuples+= (schema.name->value)
      })
      tuplesToTuples
    })
    // 遍历所有的配置规则
    array.foreach(lineIter => {
      val mapValues = lineIter
      val watch_engine = mapValues.getOrElse("watch_engine","no") // 执行引擎
      val watch_type = mapValues.getOrElse("watch_type","no") // 单表或者夺标

     val message = watch_type match {
        case "1" => //单表校验
          {
            watch_engine match {
             case "hive" =>handelerDealSingleHiveTable(spark,mapValues,batch_date,last_batch_date)
             // case "kudu" =>handelerDealSingleKuduTable(spark,mapValues, "node129:7051",batch_date)
              case _=>""
            }
          }

        case "2" => //双表校验
          watch_engine match {
            case "hive" =>handlerDealDoublehiveTable(spark,mapValues,batch_date,last_batch_date)
            case "kudu" =>
            case _=> ""
          }
      }
    if(message!=null && message!="") {
     SendEmailUtil.sendMessage(message.toString,"数据中台---线上数据校验")
      println(message)
    }
    })
    spark.close()
  }


  def clcalLastBatchDate(string: String):String={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(string)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    val lastdate = calendar.getTime
    format.format(lastdate)
  }



  /**
   * 单表kudu数据对照
   * @param spark
   * @param mapValues
   * @param batch_date
   * @return
   */
  def handelerDealSingleKuduTable(spark:SparkSession,mapValues:Map[String,String],kudu_master:String,batch_date:String,last_batch_date:String):String={
    val id = mapValues.getOrElse("id", "")
    val job_name = mapValues.getOrElse("job_name", "")
    val sql_rule_table_name = mapValues.getOrElse("sql_rule_table_name", "") // 校验表的表名
    val sql_rule = mapValues.getOrElse("sql_rule", "").replace("@{date}", s"'$batch_date'") // 获取对应的sql
    println(sql_rule)
    val kuduTablename = sql_rule_table_name.replaceAll("impala::","")
    val tempView = sql_rule_table_name.replaceAll("impala::","").replaceAll("\\.","_")
     spark.read.format("kudu").option("kudu.master", kudu_master)
      .option("kudu.table", sql_rule_table_name)
      .load().createOrReplaceTempView(tempView)
    println(sql_rule.replaceAll(kuduTablename, tempView))
    val sql_ruleDf = spark.sql(sql_rule.replaceAll(sql_rule_table_name,tempView))
    sql_ruleDf.printSchema()
   sql_ruleDf.show(100)
  ""
}





  /**
   * 单表数据校验
   * sql_rule
   * sql_rule_table_name
   * @param mapValues
   * @param batch_date
   */
  def handelerDealSingleHiveTable(spark:SparkSession,mapValues:Map[String,String],batch_date:String,last_batch_date:String)={
    val id = mapValues.getOrElse("id", "")
    val job_name = mapValues.getOrElse("job_name", "")
    val sql_rule_table_name = mapValues.getOrElse("sql_rule_table_name", "") // 校验表的表名
    val sql_rule = mapValues.getOrElse("sql_rule", "").replace("@{date}", s"'${batch_date}'").replace("@{last_date}",s"${last_batch_date}") // 获取对应的sql
    println(sql_rule)
    val  sql_rule_df= spark.sql(sql_rule).coalesce(1)
    //sql_rule_df.printSchema()
    val result = sql_rule_df.collect().map(it => {
     val buffer= new StringBuffer()
      val schemas = it.schema
      schemas.foreach(schema => {
        val value = schema.dataType.typeName match {
          case "string" => it.getAs[String](schema.name)
          case "long" => it.getAs[Long](schema.name)
          case "decimal*" => it.getAs[BigDecimal](schema.name)
          case _ => "无"
        }
        buffer.append(s"<tr>${schema.name}:$value</tr>")
      })
      buffer.toString
    }).toList
    templateMessage(id,job_name,sql_rule,"",result,sql_rule_table_name)
  }



  // 双表hive 校验
  def handlerDealDoublehiveTable(spark:SparkSession,mapValues:Map[String,String],batch_date:String,last_batch_date:String)={

    val id = mapValues.getOrElse("id", "")
    val job_name = mapValues.getOrElse("job_name", "")
    val sql_rule_table_name = mapValues.getOrElse("sql_rule_table_name", "") // 校验表的表名
    val sql_rule_watch_table_name = mapValues.getOrElse("sql_rule_watch_table_name", "") // 被参照的表的表名
    val sql_rule_watch = mapValues.getOrElse("sql_rule_watch", "").replace("@{date}", s"'${batch_date}'").replace("@{last_date}",s"${last_batch_date}") // 获取对应的sql
    println(sql_rule_watch)
    val sql_rule = mapValues.getOrElse("sql_rule", "").replace("@{date}", s"'${batch_date}'")
    println(sql_rule)
    var sql_rule_watchF = spark.sql(sql_rule_watch)
    var sql_rulef = spark.sql(sql_rule)
    var sourceMapFields = Map[String,String]() //保存原始统计字段
    // 找到两个sql 共同的维度 过滤掉统计列
    sql_rule_watchF.schema.zipWithIndex.foreach(it =>
      if (it._1.name.contains("count") || it._1.name.contains("max") || it._1.name.contains("sum")) {
        sql_rule_watchF = sql_rule_watchF.withColumnRenamed(it._1.name ,"sql_rule_watchF_" + it._2)
        sourceMapFields +=(("sql_rule_watchF_" + it._2)->it._1.name)
      }
    )
    sql_rulef.schema.zipWithIndex.foreach(it =>
      if (it._1.name.contains("count") || it._1.name.contains("max") || it._1.name.contains("sum")) {
        sql_rulef = sql_rulef.withColumnRenamed(it._1.name ,"sql_rulef_" + it._2)
        sourceMapFields +=(("sql_rulef_" + it._2)->it._1.name)
      }
    )
    val sql_rule_watchF_schema = sql_rule_watchF.schema.map(it => it.name).filter(it => {
      !it.startsWith("_")
    })
    val sql_rulef_schema = sql_rulef.schema.map(it => it.name).filter(it => {
      !it.startsWith("_")
    })
    val common_schema_name = sql_rule_watchF_schema.intersect(sql_rulef_schema).toList
    val sql_rule_watchF_index = sql_rule_watchF.schema.filter(it => {
      !common_schema_name.contains(it.name)
    }).zipWithIndex // 拉出 farme 索引
    val sql_rulef_index = sql_rulef.schema.filter(it => {
      !common_schema_name.contains(it.name)
    }).zipWithIndex // 拉出frame 索引
    val allFields = (sql_rule_watchF_index ++ sql_rulef_index).map(it => (it._2, it._1.name, it._1.dataType)) // 将两个df 的不同字段 合并
    val diffFields = allFields.groupBy(it => {
      it._1
    }).map(item => {
      val value: Int = item._1 // 小标
      val items: Seq[(Int, String, DataType)] = item._2 // 所有的相同下标的df 字段
      //组装字段到同一个tunpls
      val left_right = items.map(it => {
        it._2 + "@" + it._3.typeName.replaceAll("(decimal)(\\S+)", "$1")  // 剔除decimal的精度  为模式匹配做准备
      }).mkString("#")
      (value, left_right)
    })
    diffFields.foreach(println)
    //intToString (0,(sql_rule_watchF_-1@LongType),(sql_rulef_-1@LongType))
    // 针对frame 的维度进行对比
    //common_schema_name.foreach(println)
    import spark.implicits._
    val resultDf = sql_rule_watchF.join(sql_rulef, common_schema_name, "full").mapPartitions(iter=>{
      var res = List[(String,(String,String))]()
      while (iter.hasNext) {
        val row = iter.next()

        for (item <- diffFields) {
          val left_right: String = item._2 // 公共的字段
          if (!left_right.isEmpty) {
            val leftAndRight = left_right.split("#")
            val leftField = leftAndRight(0).split("@")(0)
            val leftType = leftAndRight(0).split("@")(1)
            val rightField = leftAndRight(1).split("@")(0)
            val rightType = leftAndRight(1).split("@")(1)
            val leftValue = leftType match {
              case "long" => row.getAs[Long](leftField)
              case "decimal"=>row.getAs[BigDecimal](leftField)
              case _=>"left"
            }
            val rightValue = rightType match {
              case "long" => row.getAs[Long](rightField)
              case "decimal" =>row.getAs[BigDecimal](rightField)
              case _ =>"right"
            }
            if (leftValue != rightValue) {
              res = (sql_rule_watch_table_name,(sourceMapFields.getOrElse(leftField,""),if(leftValue==null ) null else leftValue.toString))::res
              res = (sql_rule_table_name,(sourceMapFields.getOrElse(rightField,""),if(rightValue==null ) null else rightValue.toString))::res
              for (item <- common_schema_name) {
                res = (sql_rule_watch_table_name,(item,row.getAs[String](item)))::res
                res = (sql_rule_table_name,(item,row.getAs[String](item)))::res
              }
            }
          }
        }
      }
      res.reverse.iterator
    })
    val res: Dataset[String] = resultDf.groupByKey(it => it._1).flatMapGroups((tableName, iterator) => {
      var resList = List[String]()
      val buffer = new StringBuffer()
      buffer.append(s"\n<tr >表名：${tableName}</tr>")
      while (iterator.hasNext) {
        val tuple = iterator.next()
        val fieldName = tuple._2._1
        val fieldValue = tuple._2._2
        buffer.append(s"<tr>${fieldName}：${fieldValue}</tr>")
      }
      resList = buffer.toString::resList
      resList.iterator
    })
    val resultList= res.coalesce(1).collect().toList
      templateMessage(id,job_name,sql_rule,sql_rule_watch,resultList,"")
  }

  //组装模版消息
  def templateMessage(id:String,job_name:String,sql_rule_sql:String,sql_rule_watch_sql:String,messages:List[String],sql_rule_table_Name:String):String={
    if(messages.size>0){
    s"""
       |<table>
       |<tr>id:${id}</tr>
       |<tr>jobName:${job_name}</tr>
       |<tr>校验表sql:${sql_rule_sql}</tr>
       |<tr>参照表sql:${sql_rule_watch_sql}</tr>
       |<tr>描述:</tr>
       |${if (sql_rule_table_Name!=null && sql_rule_table_Name!="") s"<tr>表名 :${sql_rule_table_Name}</tr>" else "" }
       |${if(messages.isEmpty)"无异常数据" else messages.mkString("\n")}
       |</table>
       |""".stripMargin
    }else{
      ""
    }
  }


  //    /**
  //     * 写数据到mysql中
  //     * @param message
  //     */
  //    def writeDataToMysql(jobId:String,message:String,batchDate:String)={
  //      // 从连接池中获取mysql 连接
  //      val connection = MysqlDataSource.getConnection
  //      val presql = s"insert into flink_config.t_watch_job_message(jobId,message,batchdate) values (?,?,?)"
  //      println(presql)
  //      val statement = connection.prepareStatement(presql)
  //      statement.setString(1,s"${jobId}")
  //      statement.setString(2,s"${message}")
  //      statement.setString(3,s"${batchDate}")
  //
  //      val i = statement.executeUpdate()
  //      println(i)
  //      MysqlDataSource.close(null,statement,connection)
  //    }

}
