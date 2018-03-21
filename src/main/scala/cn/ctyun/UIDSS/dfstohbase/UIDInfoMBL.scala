/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ____________________________________________________________
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

object UIDInfoMBL extends Logging{
  var weight: String = "1"
  
  def apply(sc: SparkContext, path: String, order: String, lant_id: String): RDD[((String, String), String)] = {

    val textFile = sc.textFile(path)
    weight = order

    val result = textFile.flatMap(convert(_,lant_id))
    //println(result.collect().mkString("\n"))
    result
  }
    
  /*UID_INFO_MBL
   *   账期	MONTH_ID	STRING  一级分区
   *   归属省	PROV_ID	STRING  二级分区
   *0   归属城市	LANT_ID	STRING
   *1   用户ID	PROD_INST_ID	STRING
   *2   用户号码	ACCS_NBR	STRING
   *3   IMSI	IMSI	STRING
   *4   IMEI	IMEI	STRING
   *5   MEID	MEID	STRING
   *6   ESN	ESN	STRING
   *7   身份证号	ID_NUM	STRING
   *8 客户ID	CUST_ID	STRING
   *9 客户名称	CUST_NAME	STRING
   *10 账户ID	ACCT_ID	STRING
   *11 ICCID	ICCID	STRING
   *12 imsi注册时间	imsi_date	string
   *13 imei注册时间	imei_date	string
   *14 meid注册时间	meid_date	string
   *15 esn注册时间	esn_date	string
   *16 iccid注册时间	iccid_date	string 
   */
  def convert(line: String, lant: String): Iterable[((String, String), String)] = {
    val buf = ListBuffer[((String, String), String)]()
    try {
      val fields = line.split("\1",-1)

      if (fields.length!=17) {
        throw new RuntimeException("Table UID Info MBL has an incorrect fields length!")  
      } 
      else {
      var base = 0
      var LANT_ID = fields(base)    
      if (lant.length()>0) {
        base = -1
        LANT_ID = lant  
      }

      val PROVIN_ID = LANT_ID.substring(0, 3)
      val PROD_INST_ID = fields(base+1);
      val MBL_NBR = fields(base+2);
      val IMSI = fields(base+3);
      val IMEI = fields(base+4);
      val MEID = fields(base+5);
      val ESN = fields(base+6);
      val ID_NUM = fields(base+7);
      val CUST_ID = fields(base+8);
      val ACCT_ID = fields(base+9);
      val ICCID = fields(base+10);

      if (null != MBL_NBR && MBL_NBR.length() > 7 && MBL_NBR.length() < 100) {
        // 添加产品实例与手机号关系				
        if (null != PROD_INST_ID && PROD_INST_ID.length() > 5  && PROD_INST_ID.length() <100) {
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR, HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_PROD_INST + LANT_ID + PROD_INST_ID), weight))
          buf += (((HGraphUtil.STR_PROD_INST + LANT_ID + PROD_INST_ID, HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }
        
        // 添加手机号与身份证号关系
        if (null != ID_NUM && ID_NUM.length() > 5 && ID_NUM.length() < 100) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_ID_NUM + ID_NUM), weight))
          buf += (((HGraphUtil.STR_ID_NUM + ID_NUM,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }  

        // 添加手机号与客户ID关系
        if (null != CUST_ID && CUST_ID.length() > 5 && CUST_ID.length() < 100) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_CUST_ID + LANT_ID + CUST_ID), weight))
          buf += (((HGraphUtil.STR_CUST_ID + LANT_ID + CUST_ID,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }  		

        // 添加IMSI与手机号关系
        if (null != IMSI && IMSI.length() > 5 && IMSI.length() < 100 && (IMSI.compareToIgnoreCase("CFCD208495D565EF66E7DFF9F98764DA")!=0)) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_IMSI + IMSI), weight))
          buf += (((HGraphUtil.STR_IMSI + IMSI,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }  		
        
        // 添加IMEI与手机号关系
        if (null != IMEI && IMEI.length() > 5 && IMEI.length() < 100 && (IMEI.compareToIgnoreCase("CFCD208495D565EF66E7DFF9F98764DA")!=0)) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_IMEI + IMEI), weight))
          buf += (((HGraphUtil.STR_IMEI + IMEI,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }  		

        // 添加MEID与手机号关系
        if (null != MEID && MEID.length() > 5 && MEID.length() < 100 && (MEID.compareToIgnoreCase("CFCD208495D565EF66E7DFF9F98764DA")!=0)) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MEID + MEID), weight))
          buf += (((HGraphUtil.STR_MEID + MEID,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }  		
        
        // 添加ESN与手机号关系
        if (null != ESN && ESN.length() > 5 && ESN.length() < 100 && (ESN.compareToIgnoreCase("CFCD208495D565EF66E7DFF9F98764DA")!=0)) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_ESN + ESN), weight))
          buf += (((HGraphUtil.STR_ESN + ESN,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }
        
        // 添加ACCT_ID与手机号关系
        if (null != ACCT_ID && ACCT_ID.length() > 5 && ACCT_ID.length() < 100 ) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_ACCT_ID + LANT_ID + ACCT_ID), weight))
          buf += (((HGraphUtil.STR_ACCT_ID+ LANT_ID + ACCT_ID,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }
        
        // 添加ICCID与手机号关系
        if (null != ICCID && ICCID.length() > 5 && ICCID.length() < 100 && (ICCID.compareToIgnoreCase("CFCD208495D565EF66E7DFF9F98764DA")!=0)) { 
          buf += (((HGraphUtil.STR_MBL_NUM + MBL_NBR,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_ICCID + PROVIN_ID + ICCID), weight))
          buf += (((HGraphUtil.STR_ICCID + PROVIN_ID + ICCID,  HGraphUtil.STR_TABLE_UID_INFO_MBL + HGraphUtil.STR_MBL_NUM + MBL_NBR), weight))
        }
      }
      }
    } catch {
      case e: Exception =>
    }
    buf.toIterable
  }
}