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

object DPIInfoWB extends Logging{
  var weight: String = "1"

  def apply(sc: SparkContext, path: String, order: String): RDD[((String, String), String)] = {
    val textFile = sc.textFile(path)
    val result = textFile.flatMap(convert(_))
    //println(result.collect().mkString("\n"))
    //info("UIDSS: DPIInfoWB processed " + count + " lines.")
    result
  }

  /*  2016-3-31版本
   *0   用户号码	ACCS_NBR	STRING	　	　
   *1   QQ号1	QQ	STRING	　	　
   *2   QQ号2	QQ	STRING	　	　
   *3   QQ号3	QQ	STRING	　	　
   *4   微博号1	WEIBO	STRING	　	　
   *5   微博号2	WEIBO	STRING	　	　
   *6   微博号3	WEIBO	STRING	　	　
   *7   邮箱1	E_MAIL	STRING	　	　
   *8   邮箱2	E_MAIL	STRING	　	　
   *9   邮箱3	E_MAIL	STRING	　	　
   *10   taobao账号1	taobao_ID	STRING	　	　
   *11   taobao账号2	taobao_ID	STRING	　	　
   *12   taobao账号3	taobao_ID	STRING	　	　
   *13   jd账号1	jd_ID	STRING	　	　
   *14   jd账号2	jd_ID	STRING	　	　
   *15   jd账号3	jd_ID	STRING	　	　
   *16   手机号码(本网，异网)1	手机号码	STRING	　	　
   *17   手机号码(本网，异网)2	手机号码	STRING	　	　
   *18   手机号码(本网，异网)3	手机号码	STRING	　	　
   *19   sdkimsi1	sdkimsi	STRING	　	　
   *20   sdkimsi2	sdkimsi	STRING	　	　
   *21   sdkimsi3	sdkimsi	STRING	　	　
   *22   sdkudid1	sdkudid	STRING	　	　
   *23   sdkudid2	sdkudid	STRING	　	　
   *24   sdkudid3	sdkudid	STRING	　	　
   *25   IDFA1	IDFA	STRING	　	　
   *26   IDFA2	IDFA	STRING	　	　
   *27   IDFA3	IDFA	STRING	　	　
   *28   IMSI1	IMSI1	STRING	　	　
   *29   IMSI2	IMSI2	STRING	　	　
   *30   IMSI3	IMSI3	STRING	　	　
   *31   IMEI1	IMEI1	STRING	　	　
   *32   IMEI2	IMEI2	STRING	　	　
   *33   IMEI3	IMEI3	STRING	　	　
   *34   AndroidID1	AndroidID	STRING	　	　
   *35   AndroidID2	AndroidID	STRING	　	　
   *36   AndroidID3	AndroidID	STRING
   *37   WeChat1 	微信号1		STRING	　	　
   *38   WeChat2		微信号2 		STRING	　	　
   *39   WeChat3		微信号3 		STRING
   *40   MAC1				MAC地址1 		STRING	　	　
   *41   MAC2				MAC地址2 		STRING	　	　
   *42   MAC3				MAC地址3 		STRING
   *43		 IIM          IMSI + IMEI + MAC  STRING  
   */

  def convert(line: String): Iterable[((String, String), String)] = {
    val buf = ListBuffer[((String, String), String)]()

    try {
      val fields = line.split("\\|",-1)
      
      if (fields.length!=44) {
        throw new RuntimeException("Table DPI Info WB has an incorrect fields length!")  
      } 
      else {

      val WB_Num = fields(0);
      val QQ1 = fields(1);
      val QQ2 = fields(2);
      val QQ3 = fields(3);
      val WEIBO1 = fields(4);
      val WEIBO2 = fields(5);
      val WEIBO3 = fields(6);
      val E_MAIL1 = fields(7);
      val E_MAIL2 = fields(8);
      val E_MAIL3 = fields(9);
      val taobao_ID1 = fields(10);
      val taobao_ID2 = fields(11);
      val taobao_ID3 = fields(12);
      val jd_ID1 = fields(13);
      val jd_ID2 = fields(14);
      val jd_ID3 = fields(15);
      val MDN1 = fields(16);
      val MDN2 = fields(17);
      val MDN3 = fields(18);
      val sdkimsi1 = fields(19);
      val sdkimsi2 = fields(20);
      val sdkimsi3 = fields(21);
      val sdkudid1 = fields(22);
      val sdkudid2 = fields(23);
      val sdkudid3 = fields(24);
      val IDFA1 = fields(25);
      val IDFA2 = fields(26);
      val IDFA3 = fields(27);
      val IMSI1 = fields(28);
      val IMSI2 = fields(29);
      val IMSI3 = fields(30);
      val IMEI1 = fields(31);
      val IMEI2 = fields(32);
      val IMEI3 = fields(33);
      val AndroidID1 = fields(34);
      val AndroidID2 = fields(35);
      val AndroidID3 = fields(36);
      val WeChat1 = fields(37);
      val WeChat2 = fields(38);
      val WeChat3 = fields(39);
      val MAC1 = fields(40);
      val MAC2 = fields(41);
      val MAC3 = fields(42);
      val IIM = fields(43);
      

      if (null != WB_Num && WB_Num.length() > 7 && WB_Num.length() < 100) {

        // 添加QQ号与宽带号关系			
        if (null != QQ1 && QQ1.length() >= 5 && QQ1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_QQ + QQ1), weight))
          buf += (((HGraphUtil.STR_QQ + QQ1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != QQ2 && QQ2.length() >= 5 && QQ2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_QQ + QQ2), weight))
          buf += (((HGraphUtil.STR_QQ + QQ2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != QQ3 && QQ3.length() >= 5 && QQ3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_QQ + QQ3), weight))
          buf += (((HGraphUtil.STR_QQ + QQ3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加WEIBO与宽带号码关系
        if (null != WEIBO1 && WEIBO1.length() >= 5 && WEIBO1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WEIBO + WEIBO1), weight))
          buf += (((HGraphUtil.STR_WEIBO + WEIBO1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != WEIBO2 && WEIBO2.length() >= 5 && WEIBO2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WEIBO + WEIBO2), weight))
          buf += (((HGraphUtil.STR_WEIBO + WEIBO2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != WEIBO3 && WEIBO3.length() >= 5 && WEIBO3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WEIBO + WEIBO3), weight))
          buf += (((HGraphUtil.STR_WEIBO + WEIBO3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加E_MAIL与宽带号码关系
        if (null != E_MAIL1 && E_MAIL1.length() >= 5 && E_MAIL1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_EMAIL + E_MAIL1), weight))
          buf += (((HGraphUtil.STR_EMAIL + E_MAIL1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != E_MAIL2 && E_MAIL2.length() >= 5 && E_MAIL2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_EMAIL + E_MAIL2), weight))
          buf += (((HGraphUtil.STR_EMAIL + E_MAIL2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != E_MAIL3 && E_MAIL3.length() >= 5 && E_MAIL3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_EMAIL + E_MAIL3), weight))
          buf += (((HGraphUtil.STR_EMAIL + E_MAIL3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加taobao_ID与宽带号码关系
        if (null != taobao_ID1 && taobao_ID1.length() >= 5 && taobao_ID1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_TAOBAO_ID + taobao_ID1), weight))
          buf += (((HGraphUtil.STR_TAOBAO_ID + taobao_ID1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != taobao_ID2 && taobao_ID2.length() >= 5 && taobao_ID2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_TAOBAO_ID + taobao_ID2), weight))
          buf += (((HGraphUtil.STR_TAOBAO_ID + taobao_ID2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != taobao_ID3 && taobao_ID3.length() >= 5 && taobao_ID3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_TAOBAO_ID + taobao_ID3), weight))
          buf += (((HGraphUtil.STR_TAOBAO_ID + taobao_ID3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加jd_ID与宽带号码关系
        if (null != jd_ID1 && jd_ID1.length() >= 5 && jd_ID1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_JD_ID + jd_ID1), weight))
          buf += (((HGraphUtil.STR_JD_ID + jd_ID1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != jd_ID2 && jd_ID2.length() >= 5 && jd_ID2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_JD_ID + jd_ID2), weight))
          buf += (((HGraphUtil.STR_JD_ID + jd_ID2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != jd_ID3 && jd_ID3.length() >= 5 && jd_ID3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_JD_ID + jd_ID3), weight))
          buf += (((HGraphUtil.STR_JD_ID + jd_ID3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加MDN与宽带号码关系
        if (null != MDN1 && MDN1.length() >= 5 && MDN1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN1), weight))
          buf += (((HGraphUtil.STR_MBL_NUM + MDN1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != MDN2 && MDN2.length() >= 5 && MDN2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN2), weight))
          buf += (((HGraphUtil.STR_MBL_NUM + MDN2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != MDN3 && MDN3.length() >= 5 && MDN3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MBL_NUM + MDN3), weight))
          buf += (((HGraphUtil.STR_MBL_NUM + MDN3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加sdkimsi与宽带号码关系
        if (null != sdkimsi1 && sdkimsi1.length() >= 5 && sdkimsi1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkimsi + sdkimsi1), weight))
          buf += (((HGraphUtil.STR_sdkimsi + sdkimsi1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != sdkimsi2 && sdkimsi2.length() >= 5 && sdkimsi2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkimsi + sdkimsi2), weight))
          buf += (((HGraphUtil.STR_sdkimsi + sdkimsi2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != sdkimsi3 && sdkimsi3.length() >= 5 && sdkimsi3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkimsi + sdkimsi3), weight))
          buf += (((HGraphUtil.STR_sdkimsi + sdkimsi3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加sdkudid与宽带号码关系
        if (null != sdkudid1 && sdkudid1.length() >= 5 && sdkudid1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkudid + sdkudid1), weight))
          buf += (((HGraphUtil.STR_sdkudid + sdkudid1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != sdkudid2 && sdkudid2.length() >= 5 && sdkudid2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkudid + sdkudid2), weight))
          buf += (((HGraphUtil.STR_sdkudid + sdkudid2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != sdkudid3 && sdkudid3.length() >= 5 && sdkudid3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_sdkudid + sdkudid3), weight))
          buf += (((HGraphUtil.STR_sdkudid + sdkudid3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加IDFA与宽带号码关系
        if (null != IDFA1 && IDFA1.length() >= 5 && IDFA1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IDFA + IDFA1), weight))
          buf += (((HGraphUtil.STR_IDFA + IDFA1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IDFA2 && IDFA2.length() >= 5 && IDFA2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IDFA + IDFA2), weight))
          buf += (((HGraphUtil.STR_IDFA + IDFA2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IDFA3 && IDFA3.length() >= 5 && IDFA3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IDFA + IDFA3), weight))
          buf += (((HGraphUtil.STR_IDFA + IDFA3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加IMSI与宽带号码关系
        if (null != IMSI1 && IMSI1.length() >= 5 && IMSI1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMSI + IMSI1), weight))
          buf += (((HGraphUtil.STR_IMSI + IMSI1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IMSI2 && IMSI2.length() >= 5 && IMSI2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMSI + IMSI2), weight))
          buf += (((HGraphUtil.STR_IMSI + IMSI2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IMSI3 && IMSI3.length() >= 5 && IMSI3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMSI + IMSI3), weight))
          buf += (((HGraphUtil.STR_IMSI + IMSI3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加IMEI与宽带号码关系
        if (null != IMEI1 && IMEI1.length() >= 5 && IMEI1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMEI + IMEI1), weight))
          buf += (((HGraphUtil.STR_IMEI + IMEI1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IMEI2 && IMEI2.length() >= 5 && IMEI2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMEI + IMEI2), weight))
          buf += (((HGraphUtil.STR_IMEI + IMEI2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != IMEI3 && IMEI3.length() >= 5 && IMEI3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IMEI + IMEI3), weight))
          buf += (((HGraphUtil.STR_IMEI + IMEI3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加AndroidID与宽带号码关系
        if (null != AndroidID1 && AndroidID1.length() >= 5 && AndroidID1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_AndroidID + AndroidID1), weight))
          buf += (((HGraphUtil.STR_AndroidID + AndroidID1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != AndroidID2 && AndroidID2.length() >= 5 && AndroidID2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_AndroidID + AndroidID2), weight))
          buf += (((HGraphUtil.STR_AndroidID + AndroidID2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != AndroidID3 && AndroidID3.length() >= 5 && AndroidID3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_AndroidID + AndroidID3), weight))
          buf += (((HGraphUtil.STR_AndroidID + AndroidID3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加微信号与宽带号关系			
        if (null != WeChat1 && WeChat1.length() >= 5 && WeChat1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WE_CHAT + WeChat1), weight))
          buf += (((HGraphUtil.STR_WE_CHAT  + WeChat1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != WeChat2 && WeChat2.length() >= 5 && WeChat2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WE_CHAT  + WeChat2), weight))
          buf += (((HGraphUtil.STR_WE_CHAT  + WeChat2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != WeChat3 && WeChat3.length() >= 5 && WeChat3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WE_CHAT  + WeChat3), weight))
          buf += (((HGraphUtil.STR_WE_CHAT  + WeChat3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }

        // 添加MAC与宽带号关系			
        if (null != MAC1 && MAC1.length() >= 5 && MAC1.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MAC + MAC1), weight))
          buf += (((HGraphUtil.STR_MAC  + MAC1, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != MAC2 && MAC2.length() >= 5 && MAC2.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MAC  + MAC2), weight))
          buf += (((HGraphUtil.STR_MAC  + MAC2, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }
        if (null != MAC3 && MAC3.length() >= 5 && MAC3.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_MAC  + MAC3), weight))
          buf += (((HGraphUtil.STR_MAC  + MAC3, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
        }        
        
        // 添加IIM与宽带号关系			
        if (null != IIM && IIM.length() >= 5 && IIM.length() < 100) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_Num, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_IIM + IIM), weight))
          buf += (((HGraphUtil.STR_IIM  + IIM, HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT + HGraphUtil.STR_WB_NUM + WB_Num), weight))
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