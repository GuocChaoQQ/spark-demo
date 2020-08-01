package com.demo.utils


import org.apache.kudu.client.{KuduClient}

/**
 * created by chao.guo on 2020/8/1
 **/
object KuduUtils {
  var kuduClient :KuduClient = _;

  /**
   * 获取kudu Client
   * @param param
   * @return
   */
  def getKuduClent(param: Map[String,String]):KuduClient={
    if(kuduClient==null ){
      kuduClient= new KuduClient.KuduClientBuilder(param("kudu_master")).build()

      println("init kuduclient")
    }
     kuduClient
  }

}
