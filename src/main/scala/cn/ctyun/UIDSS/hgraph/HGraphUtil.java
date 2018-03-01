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
package cn.ctyun.UIDSS.hgraph;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * 类描述：HBase图结构常量定义
 */
public class HGraphUtil {
	public static final byte[] COLUMN_FAMILY = {'v'};  // COLUMN_FAMILY 是 "v"
	
	public static final byte[] BYTE_TABLE_ENCODE_EQ =  {'$','0'};  //不同加密等价关系
	public static final byte[] BYTE_TABLE_UID_INFO_MBL =  {'$','1'};  //--C网用户UID信息表 
	public static final byte[] BYTE_TABLE_UID_INFO_TEL =  {'$','2'}; //--固网电话用户UID信息表
	public static final byte[] BYTE_TABLE_UID_INFO_WB =  {'$','3'}; //--固网宽带用户UID信息表 
	public static final byte[] BYTE_TABLE_UID_MATCH_USER = {'A', '1'} ; //--匹配用户 
	public static final byte[] BYTE_TABLE_UID_FIX_DPI_USER_ACCOUNT = {'a', '1'}; //--固网DPI网络侧用户账号  
	public static final byte[] BYTE_TABLE_UID_OTH_DPI_USER_ACCOUNT =  {'a', '2'}; //--移动DPI网络侧用户账号 30002 \x75  u ; \x32 2

	public static final byte[] BYTE_TABLE_UID=  {'z', 'z'}; //--UID表

	public static final String STR_PROD_INST ="PI";  // 产品实例是 "PI"
	public static final String STR_ACCS_NUM = "AN";  // 固网接入号码是 "AN"
	public static final String STR_MBL_NUM = "MN";  // 移动号码是 "MN"
	public static final String STR_WB_NUM = "WN";  // 宽带号码是 "WN"
	public static final String STR_IMSI = "IS";  // IMSI是 "IS"
	public static final String STR_IMEI = "IE";  // IMEI是 "IE"
	public static final String STR_MEID = "ME";  // MEID 是 "ME"
	public static final String STR_ESN = "EN";  // ESN是 "EN"
	public static final String STR_ID_NUM = "ID";  // 身份证号是 "ID"
	public static final String STR_CUST_ID = "CI";  // 客户ID是 "CI"
	public static final String STR_ACCT_ID = "AI";  // 帐户ID是 "AI"
	public static final String STR_ICCID = "II";  // ICCID是 "II"
	
	public static final String STR_QQ = "QQ";  // QQ号码是 "QQ"
	public static final String STR_EMAIL = "ML";  // email是 "ML"
	
	public static final String STR_WE_CHAT= "WC";  // 微信是 "WC"
	public static final String STR_WEIBO= "WB";  // 微博是 "WB"
	public static final String STR_sdkimsi= "SI";  // sdkimsi是 "SI"
	public static final String STR_sdkudid= "SU";  // sdkudid是 "SU"
	public static final String STR_TAOBAO_ID= "TI";  // 淘宝是 "TI"
	public static final String STR_JD_ID= "JI";  // 京东是 "JI"
	public static final String STR_IDFA = "IA";  // IDFA是 "IA"
	public static final String STR_AndroidID = "AD";  // AndroidID是 "AD"
	public static final String STR_MAC= "MC"; //设备MAC地址
	public static final String STR_IIM1 = "IM1";  // IMEI + IMSI + MAC1.
	public static final String STR_IIM2 = "IM2";  // IMEI + IMSI + MAC2.
	public static final String STR_IIM3 = "IM3";  // IMEI + IMSI + MAC3.
	
	public static final String STR_UD = "UD";  // UID是 "UD"
	
	public static final byte[] BYTE_PROD_INST = {'P','I'};  // 产品实例是 "PI"
	public static final byte[] BYTE_ACCS_NUM = {'A','N'};  // 接入号码是 "AN"
	public static final byte[] BYTE_MBL_NUM =  {'M','N'};  // 移动号码是 "MN"
	public static final byte[] BYTE_WB_NUM =  {'W','N'};  // 宽带号码是 "WN"
	public static final byte[] BYTE_IMSI = {'I','S'};  // IMSI是 "IS"
	public static final byte[] BYTE_IMEI = {'I','E'};  // IMEI是 "IE"
	public static final byte[] BYTE_MEID = {'M','E'};  // MEID 是 "ME"
	public static final byte[] BYTE_ESN = {'E','N'};  // ESN是 "EN"
	public static final byte[] BYTE_ID_NUM = {'I','D'};  // 身份证号是 "ID"
	public static final byte[] BYTE_CUST_ID = {'C','I'};  // 客户ID是 "CI"
	public static final byte[] BYTE_ACCT_ID = {'A','I'};  // 帐户ID是 "AI"
	public static final byte[] BYTE_ICCID = {'I','I'};  // ICCID是 "II"
	public static final byte[] BYTE_QQ = {'Q','Q'};  // QQ号码是 "QQ"
	public static final byte[] BYTE_EMAIL = {'M','L'};  // email是 "ML"
	
	public static final byte[] BYTE_WE_CHAT= {'W','C'};  // 微信是 "WC"
	public static final byte[] BYTE_WEIBO= {'W','B'};  // 微博是 "WB"
	public static final byte[] BYTE_sdkimsi= {'S','I'};  // sdkimsi是 "SI"
	public static final byte[] BYTE_sdkudid= {'S','U'};  // sdkudid是 "SU"
	public static final byte[] BYTE_TAOBAO_ID= {'T','I'};  // 淘宝是 "TI"
	public static final byte[] BYTE_JD_ID= {'J','I'};  // 京东是 "JI"
	public static final byte[] BYTE_IDFA_AndroidID = {'I','A'};  // IDFA_AndroidID是 "IA"
	public static final byte[] BYTE_IDFA = {'I','A'};  // IDFA是 "IA"
	public static final byte[] BYTE_AndroidID = {'A','D'};  // AndroidID是 "AD"
	public static final byte[] BYTE_MAC = {'M','C'};  // 设备MAC地址
	public static final byte[] BYTE_IIM = {'I','M'};  // IMEI + IMSI + MAC.
	
	public static final byte[] BYTE_UID = {'U','D'};  // UID是 "UD"
	
	public static final byte[] BYTE_UID_BASE ={ 'b', 'a', 's', 'e' };;  // UID Base 列是 "base"
	
	public static final short TABLE_ENCODE_EQ =  Bytes.toShort(BYTE_TABLE_ENCODE_EQ);
	public static final short TABLE_UID_INFO_MBL =  Bytes.toShort(BYTE_TABLE_UID_INFO_MBL);
	public static final short TABLE_UID_INFO_TEL = Bytes.toShort(BYTE_TABLE_UID_INFO_TEL);
	public static final short TABLE_UID_INFO_WB = Bytes.toShort(BYTE_TABLE_UID_INFO_WB); 
	public static final short TABLE_UID_MATCH_USER = Bytes.toShort(BYTE_TABLE_UID_MATCH_USER);
	public static final short TABLE_UID_FIX_DPI_USER_ACCOUNT = Bytes.toShort(BYTE_TABLE_UID_FIX_DPI_USER_ACCOUNT);
	public static final short TABLE_UID_OTH_DPI_USER_ACCOUNT = Bytes.toShort(BYTE_TABLE_UID_OTH_DPI_USER_ACCOUNT);

	public static final short TABLE_UID = Bytes.toShort(BYTE_TABLE_UID);//--UID表

	public static final String STR_TABLE_ENCODE_EQ =  "$0";  //不同加密等价关系
	public static final String STR_TABLE_UID_INFO_MBL =  "$1";  //--C网用户UID信息表 
	public static final String STR_TABLE_UID_INFO_TEL =  "$2"; //--固网电话用户UID信息表
	public static final String STR_TABLE_UID_INFO_WB =  "$3"; //--固网宽带用户UID信息表 
	public static final String STR_TABLE_UID_MATCH_USER = "A1" ; //--匹配用户 
	public static final String STR_TABLE_UID_FIX_DPI_USER_ACCOUNT = "a1"; //--固网DPI网络侧用户账号  
	public static final String STR_TABLE_UID_OTH_DPI_USER_ACCOUNT =  "a2"; //--移动DPI网络侧用户账号 30002 \x75  u ; \x32 2

	public static final String STR_TABLE_UID=  "zz"; //--UID表
	
	
	public static final short CLMN_PROD_INST = Bytes.toShort(BYTE_PROD_INST);  // 产品实例是 "PI"
	public static final short CLMN_ACCS_NUM = Bytes.toShort(BYTE_ACCS_NUM);  // 接入号码是 "AN"
	public static final short CLMN_MBL_NUM = Bytes.toShort(BYTE_MBL_NUM);  // 移动号码是 "MN"
	public static final short CLMN_WB_NUM = Bytes.toShort(BYTE_WB_NUM);  // 宽带号码是 "WN"
	public static final short CLMN_IMSI = Bytes.toShort(BYTE_IMSI);  // IMSI是 "IS"
	public static final short CLMN_IMEI = Bytes.toShort(BYTE_IMEI);  // IMEI是 "IE"
	public static final short CLMN_MEID =Bytes.toShort(BYTE_MEID);  // MEID 是 "ME"
	public static final short CLMN_ESN = Bytes.toShort(BYTE_ESN);  // ESN是 "EN"
	public static final short CLMN_ID_NUM = Bytes.toShort(BYTE_ID_NUM);  // 身份证号是 "ID"
	public static final short CLMN_CUST_ID =Bytes.toShort(BYTE_CUST_ID);  // 客户ID是 "CI"
	public static final short CLMN_ACCT_ID =Bytes.toShort(BYTE_ACCT_ID);  // 帐户ID是 "AI"
	public static final short CLMN_ICCID = Bytes.toShort(BYTE_ICCID);  // ICCID是 "II"
	public static final short CLMN_QQ =Bytes.toShort(BYTE_QQ);  // QQ号码是 "QQ"
	public static final short CLMN_EMAIL =Bytes.toShort(BYTE_EMAIL);  // email是 "ML"
	
	public static final short CLMN_WE_CHAT=Bytes.toShort(BYTE_WE_CHAT);  // 微信是 "WC"
	public static final short CLMN_WEIBO= Bytes.toShort(BYTE_WEIBO);  // 微博是 "WB"
	public static final short CLMN_sdkimsi= Bytes.toShort(BYTE_sdkimsi);  // sdkimsi是 "SI"
	public static final short CLMN_sdkudid=Bytes.toShort(BYTE_sdkudid);  // sdkudid是 "SU"
	public static final short CLMN_TAOBAO_ID= Bytes.toShort(BYTE_TAOBAO_ID);  // 淘宝是 "TI"
	public static final short CLMN_JD_ID= Bytes.toShort(BYTE_JD_ID);  // 京东是 "JI"
	public static final short CLMN_IDFA =Bytes.toShort(BYTE_IDFA);  // IDFA是 "IA"
	public static final short CLMN_AndroidID =Bytes.toShort(BYTE_AndroidID);  // AndroidID是 "AD"
	public static final short CLMN_MAC = Bytes.toShort(BYTE_MAC);  // 设备MAC地址
	public static final short CLMN_IIM = Bytes.toShort(BYTE_IIM);  // IMEI + IMSI + MAC.	
	
	public static final short CLMN_UID =Bytes.toShort(BYTE_UID);  // UID 是 "UD"
	
	public static byte[] IDToRowkey(short iNodeType, String strNodeID){
		//需要补齐2位的类型标志
		byte[] typeBytes = (iNodeType < 255) ? Bytes.add(Bytes.toBytes(0),Bytes.toBytes(iNodeType)) : Bytes.toBytes(iNodeType) ;
		byte[] nodeIDBytes = strNodeID.getBytes();
	    return  Bytes.add(typeBytes,nodeIDBytes);
	}
	
	public static byte[] IDToRowkey(String nodeType, String strNodeID){
		//需要补齐2位的类型标志
		byte[] typeBytes = Bytes.toBytes(nodeType) ;
		byte[] nodeIDBytes = strNodeID.getBytes();
	    return  Bytes.add(typeBytes,nodeIDBytes);
	}
	
	public static String TypeFromRowkey(byte[] rowkey){
		//需要补齐2位的类型标志
		byte[] nodeIDBytes =  Bytes.copy(rowkey, 0, 2) ;
	    return  Bytes.toString(nodeIDBytes);
	}
	
	public static byte[] TypeByteFromRowkey(byte[] rowkey){
		//需要补齐2位的类型标志
	    return  Bytes.copy(rowkey, 0, 2) ;
	}
	
	public static String IDFromRowkey(byte[] rowkey){
		//需要补齐2位的类型标志
		byte[] typeBytes = Bytes.copy(rowkey, 2, rowkey.length-2) ;
	    return  Bytes.toString(typeBytes);
	}
	
	public static byte[] IDToCQ(short iRelationType, short iNodeType, String strNodeID){
		//需要补齐2位的关系类型标志
		byte[] relationBytes = (iRelationType < 255) ? Bytes.add(Bytes.toBytes(0),Bytes.toBytes(iRelationType)) : Bytes.toBytes(iRelationType) ;
		//需要补齐2位的节点类型标志
		byte[] typeBytes = (iNodeType < 255) ? Bytes.add(Bytes.toBytes(0),Bytes.toBytes(iNodeType)) : Bytes.toBytes(iNodeType) ;
		typeBytes =  Bytes.add(relationBytes,typeBytes);
		byte[] nodeIDBytes = strNodeID.getBytes();
	    return  Bytes.add(typeBytes,nodeIDBytes);
	}
	
	public static byte[] RowkeyToCQ(short iRelationType,byte[] rowkey){
		//需要补齐2位的关系类型标志
		byte[] relationBytes = (iRelationType < 255) ? Bytes.add(Bytes.toBytes(0),Bytes.toBytes(iRelationType)) : Bytes.toBytes(iRelationType) ;
	    return  Bytes.add(relationBytes,rowkey);
	}
	
	public static byte[] IDFromCQ(byte[] cq){
		//需要除去2位的关系类型标志
	    return  Bytes.copy(cq, 2, cq.length-2)  ;
	}

	public static byte[] TypeFromCQ(byte[] cq){
		//取出2位的关系类型标志
	    return  Bytes.copy(cq, 0, 2)  ;
	}

	
}
