package com.demo.utils

/**
 * created by chao.guo on 2020/8/29
 *
 **/
import java.util.{Date, Properties}

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session, Transport}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SendEmailUtil {


  var session:Session =_;
  var pro:Properties= new  Properties();
  var transport:Transport=_;
  def main(args: Array[String]): Unit = {

//    //2、创建定义整个应用程序所需的环境信息的 Session 对象
//    val session: Session = Session.getInstance(props)
//    //设置调试信息在控制台打印出来
//    //session.setDebug(true)
//    //3、创建邮件的实例对象
//    val msg: MimeMessage = getMimeMessage(session)
//    //4、根据session对象获取邮件传输对象Transport
//    val transport: Transport = session.getTransport
//    transport.connect()
//    //设置发件人的账户名和密码
//   // transport.connect(senderAccount, senderPassword)
//    //发送邮件，并发送到所有收件人地址，message.getAllRecipients() 获取到的是在创建邮件对象时添加的所有收件人, 抄送人, 密送人
//    transport.sendMessage(msg, msg.getAllRecipients)
//
//
//    //5、关闭邮件连接
//   transport.close()

  }

  def initEmailSession(spark:SparkSession, configPath:String) ={
    val inputSteam = FileSystem.get(spark.sparkContext.hadoopConfiguration).open(new Path(configPath))
    pro.load(inputSteam)
    inputSteam.close()
    session = Session.getInstance(pro)
    transport = session.getTransport
  }



  /** 发送邮件信息
   *
   * @param
   * @param subjectType
   */
  def  sendMessage(messag:String,subjectType:String): Unit ={
    val senderAccount = pro.getProperty("senderAccount")
    val senderPassword = pro.getProperty("senderPassword")
    if(!senderAccount.isEmpty && !senderPassword.isEmpty){
      transport.connect(senderAccount,senderPassword)
    }else{
      transport.connect();
    }
    // 拼装模版消息
    val msg = new MimeMessage(session)
    msg.setFrom(new InternetAddress(pro.getProperty("senderAddress")))

    msg.setFrom(new InternetAddress(pro.getProperty("sendFrom")))
    msg.addRecipients(Message.RecipientType.TO,pro.getProperty("recipientAddress"))
    msg.setSubject(subjectType, "UTF-8")
    //设置邮件正文
    msg.setContent(messag, "text/html;charset=UTF-8")
    //设置邮件的发送时间,默认立即发送
    msg.setSentDate(new Date())
    transport.sendMessage(msg,msg.getAllRecipients)
    transport.close() //关闭连接
}



}
