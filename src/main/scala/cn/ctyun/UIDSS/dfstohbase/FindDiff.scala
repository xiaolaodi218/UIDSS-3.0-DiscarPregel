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

import org.apache.spark.rdd.RDD
import cn.ctyun.UIDSS.utils.{Logging }

object FindDiff extends Logging{
  def apply(rddOld: RDD[((String, String), String)], rddNew: RDD[((String, String), String)]): RDD[((String, String), String)] = {

    val rddAdd = rddNew.subtract(rddOld)
    //println("rddAdd is: \n" +  rddAdd.collect().mkString("\n"))
    
    
    val rddRemove = rddOld.subtract(rddNew).map(row => (row._1, "0")) //把需要删除的记录，权重置为 0
    //println("rddRemove is: \n" +  rddRemove.collect().mkString("\n"))

    rddAdd++rddRemove
  }
}