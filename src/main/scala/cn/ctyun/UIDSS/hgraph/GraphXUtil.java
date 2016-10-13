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


/**
 * 类描述：GraphX
 */
public class GraphXUtil {
	// Long.MaxValue   最多9223372036854775807
	public static final long MAX_VERTICE = 1000000000000000L;    //最多1000万亿, 也就是10万个分区
	public static final long MAX_VERTICE_PER_PARTITION = 10000000000L;  //每个分区最多100亿个, 1000G内存
}
