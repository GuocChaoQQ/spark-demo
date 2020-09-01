package com.demo.utils

/**
 * created by chao.guo on 2020/8/28
 **/
import java.sql.{Connection, ResultSet, Statement}
import java.util.Properties

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object MysqlDataSource {
  var druidDataSource: DruidDataSource = _;
  def initDataSource(spark:SparkSession, configPath:String)={
    val pro = new  Properties()
    val inputSteam = FileSystem.get(spark.sparkContext.hadoopConfiguration).open(new Path(configPath))
    pro.load(inputSteam)
    inputSteam.close()
    druidDataSource =DruidDataSourceFactory.createDataSource(pro).asInstanceOf[DruidDataSource]
  }



  // 获取连接
  def getConnection: Connection = {
      druidDataSource.getConnection
  }

  //关闭连接
  def close(rs: ResultSet, statement: Statement, conn: Connection): Unit = {
    try {
      if (null != rs) rs.close
      if (null != statement) statement.close
      if (null != conn) conn.close
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}
