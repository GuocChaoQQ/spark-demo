package com.demo.utils

/**
 * created by chao.guo on 2020/8/29
 * id job id
 * jobName jobName
 * titles: 标题头
 * values：values值
 *
 **/
case class MessageMode(values:Map[String,String])

case class MessageJobMode(i:String,jobName:String,batch_date:String,sql_rule_table:String,sql_rule_watch_table:String,messages:List[MessageMode])

// jobid:  jobname:  时间
// 参照表：公共字段    （表名）维度1     表名（2）维度1  （表名1）维度2     表名（2）维度2 （表名1）维度1     表名（2）维度2

