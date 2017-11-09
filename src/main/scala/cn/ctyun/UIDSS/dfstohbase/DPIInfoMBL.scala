/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
 * 
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package cn.ctyun.UIDSS.dfstohbase

import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

import cn.ctyun.UIDSS.hgraph.HGraphUtil
import cn.ctyun.UIDSS.utils.{Logging }

object DPIInfoMBL extends Logging{
  var weight: String = "1"

  def apply(sc: SparkContext, path: String, order: String): RDD[((String, String), String)] = {
    val textFile = sc.textFile(path)
    val result = textFile.flatMap(convert(_))
    //println(result.collect().mkString("\n"))
    //info("DPIInfoMBL processed " + count + " lines.")
    result
  }

  /* 2016-3-31版本
   *0  	MDN	MDN	STRING	　	　
   *1  	IMSI	IMSI	STRING	　	　
   *2  	MEID	MEID	STRING	　	　
   *3  	QQ号	QQ	STRING	如存在对应多个网络账号，取出现频次最高的账号	　
   *4  	微博号	WEIBO	STRING	如存在对应多个网络账号，取出现频次最高的账号	　
   *5  	邮箱	E_MAIL	STRING	如存在对应多个网络账号，取出现频次最高的账号	　
   *6  	sdkimsi	sdkimsi	STRING	　	　
   *7  	sdkudid	sdkudid	STRING	　	　
   *8  	taobao账号	taobao_ID	STRING	如存在对应多个网络账号，取出现频次最高的账号	　
   *9  	jd账号	jd_ID	STRING	如存在对应多个网络账号，取出现频次最高的账号	　
   *10  IDFA	IDFA	STRING	　	　
   *11  AndroidID	AndroidID	STRING
   *12		WeChat WeChat	STRING
   *13		MAC MAC	STRING
   *14		IMEI IMEI	STRING     
  */

  def convert(line: String): Iterable[((String, String), String)] = {
    val buf = ListBuffer[((String, String), String)]()

    try {
      val fields = line.split("\\|",-1)

      if (fields.length!=15) {
        throw new RuntimeException("Table DPI Info MBL has an incorrect fields length!")  
      } 
      else {      
      val MDN = fields(0);
      val IMSI = fields(1);
      val MEID = fields(2);
      val QQ = fields(3);
      val WEIBO = fields(4);
      val E_MAIL = fields(5);
      val sdkimsi = fields(6);
      val sdkudid = fields(7);
      val taobao_ID = fields(8);
      val jd_ID = fields(9);
      val IDFA = fields(10);
      val AndroidID = fields(11);
      val WeChat = fields(12);
      val MAC = fields(13);
      val IMEI = fields(14);      

      if (null != MDN && MDN.length() > 7 && MDN.length() < 100) {

        // 添加QQ号与手机号关系			
        if (null != QQ && QQ.length() >= 5 && QQ.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_QQ + QQ), weight))
          buf += (((HGraphUtil.STR_QQ + QQ, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }

        // 添加IMSI与电话号码关系
        if (null != IMSI && IMSI.length() >= 5 && IMSI.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_IMSI + IMSI), weight))
          buf += (((HGraphUtil.STR_IMSI + IMSI, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }

        // 添加MEID与电话号码关系
        if (null != MEID && MEID.length() >= 5 && MEID.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MEID + MEID), weight))
          buf += (((HGraphUtil.STR_MEID + MEID, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }

        // 添加WEIBO与电话号码关系
        if (null != WEIBO && WEIBO.length() >= 5 && WEIBO.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_WEIBO + WEIBO), weight))
          buf += (((HGraphUtil.STR_WEIBO + WEIBO, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }

        // 添加E_MAIL与电话号码关系
        if (null != E_MAIL && E_MAIL.length() >= 5 && E_MAIL.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_EMAIL + E_MAIL), weight))
          buf += (((HGraphUtil.STR_EMAIL + E_MAIL, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }

        // 添加sdkimsi与电话号码关系
        if (null != sdkimsi && sdkimsi.length() >= 5 && sdkimsi.length() < 100 ) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkimsi + sdkimsi), weight))
          buf += (((HGraphUtil.STR_sdkimsi + sdkimsi, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }        
        
        // 添加sdkudid与电话号码关系
        if (null != sdkudid && sdkudid.length() >= 5 && sdkudid.length() <100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkudid + sdkudid), weight))
          buf += (((HGraphUtil.STR_sdkudid + sdkudid, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }                

        // 添加taobao_ID与电话号码关系
        if (null != taobao_ID && taobao_ID.length() >= 5 && taobao_ID.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_TAOBAO_ID + taobao_ID), weight))
          buf += (((HGraphUtil.STR_TAOBAO_ID + taobao_ID, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }                

        // 添加jd_ID与电话号码关系
        if (null != jd_ID && jd_ID.length() >= 5 && jd_ID.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_JD_ID + jd_ID), weight))
          buf += (((HGraphUtil.STR_JD_ID + jd_ID, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }                   

        // 添加IDFA与电话号码关系
         if (null != IDFA && IDFA.length() >= 5 && IDFA.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_IDFA + IDFA), weight))
          buf += (((HGraphUtil.STR_IDFA + IDFA, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }          
         
        // 添加AndroidID与电话号码关系
        if (null != AndroidID && AndroidID.length() >= 5 && AndroidID.length() < 100) {          	
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_AndroidID + AndroidID), weight))
          buf += (((HGraphUtil.STR_AndroidID + AndroidID, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }          

        // 添加微信号与手机号关系			
        if (null != WeChat && WeChat.length() >= 5 && WeChat.length() < 100) {
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_WE_CHAT + WeChat), weight))
          buf += (((HGraphUtil.STR_WE_CHAT  + WeChat, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }         

        // 添加MAC与手机号关系			
        if (null != MAC && MAC.length() >= 5 && MAC.length() < 100) {
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MAC + MAC), weight))
          buf += (((HGraphUtil.STR_MAC  + MAC, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }   
        
        // 添加IMEI与手机号关系			
        if (null != IMEI && IMEI.length() >= 5 && IMEI.length() < 100) {
          buf += (((HGraphUtil.STR_MBL_NUM + MDN, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_IMEI + IMEI), weight))
          buf += (((HGraphUtil.STR_IMEI  + IMEI, HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN), weight))
        }           
        count += 1
      }
      }
    } catch {
      case e: Exception =>
    }
    buf.toIterable
  }
}