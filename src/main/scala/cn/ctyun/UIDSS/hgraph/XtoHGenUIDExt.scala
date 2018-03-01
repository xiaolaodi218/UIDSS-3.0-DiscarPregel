/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ________________________________________________________
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
package cn.ctyun.UIDSS.hgraph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes;

import scala.collection.mutable.ListBuffer

/**
 * 类描述：
 */
object XtoHGenUIDExt {
  def getNewRelations(uid: String, group: List[(String, String)]): Iterable[((String, String), String)] = {

    val buf = new ListBuffer[((String, String), String)]
    for (vertex <- group) {
      val typ = vertex._1.substring(0, 2)
      if (typ.compareTo(HGraphUtil.STR_UD) != 0 //因为加的是双向只需要从非UID节点查起就够了
        && ((typ.compareTo(HGraphUtil.STR_ACCS_NUM) == 0 //只需要关键节点加UID关联
          || typ.compareTo(HGraphUtil.STR_MBL_NUM) == 0
          || typ.compareTo(HGraphUtil.STR_WB_NUM) == 0
          || typ.compareTo(HGraphUtil.STR_QQ) == 0
          || typ.compareTo(HGraphUtil.STR_WE_CHAT) == 0
          || typ.compareTo(HGraphUtil.STR_CUST_ID) == 0
          || typ.compareTo(HGraphUtil.STR_ID_NUM) == 0))) {

        val strUIDTable = Bytes.toString(HGraphUtil.BYTE_TABLE_UID)
        if (vertex._2.length() > 0) { //原来有UID，有可能需要替换
          if (vertex._2.compareTo(uid) != 0) { //新旧UID是不一样的需要替换
            buf += (((vertex._1, strUIDTable + vertex._2), "0"))
            buf += (((vertex._2, strUIDTable + vertex._1), "0"))

            buf += (((vertex._1, strUIDTable + uid), "1"))
            buf += (((uid, strUIDTable + vertex._1), "1"))
          }
        } else { //原来没有UID，新生成     
          buf += (((vertex._1, strUIDTable + uid), "1"))
          buf += (((uid, strUIDTable + vertex._1), "1"))
        }
      }

    }
    buf.toIterable
  }

  def apply(rddGroupWithUID: RDD[(String, List[(String, String)])]): RDD[((String, String), String)] = {
    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
    rddGroupWithUID.flatMap { case (uid, group) => getNewRelations(uid, group) }
  }
}