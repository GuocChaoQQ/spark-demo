package com.demo.utils

import org.apache.spark.sql.SparkSession

/**
 * created by chao.guo on 2020/4/8
 **/
object HiveUtils {
def getSparkSession( appName:String,masterUrl:String):SparkSession ={
  SparkSession.builder().master(masterUrl).
    appName(appName)
//    config("spark.sql.warehouse.dir", "hdfs://10.83.0.47:8020/user/hive/warehouse")
    .config("spark.some.config.option", "some-value")
    .config("spark.sql.adaptive.enabled","true")
    .config("spark.sql.shuffle.partitions","10")
    .enableHiveSupport().getOrCreate()
}


}
