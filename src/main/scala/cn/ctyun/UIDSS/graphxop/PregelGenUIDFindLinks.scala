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

package cn.ctyun.UIDSS.graphxop

import org.apache.spark.graphx._
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

//找出图中所有节点关联的UID节点,并记录到节点的UID属性中
object PregelGenUIDFindLinks {

  //广播找出关联的UID
  def sendMsg_UID(triplet: EdgeTriplet[(String, (Long, String)), (String, Long)]): Iterator[(VertexId, String)] = {
    val typ = triplet.srcAttr._1.substring(0, 2)
    if (triplet.srcAttr._2._1 == triplet.dstId || typ.compareTo(HGraphUtil.STR_UD) != 0) {
      //println(triplet.srcAttr._2 + " is terminated " )    
      Iterator.empty //如果当前节点值==目标节点, 不用再遍历
    } else {
      //println( triplet.dstId  + " is sent message  " + triplet.srcAttr._2 )
      Iterator((triplet.dstId, triplet.srcAttr._1)) //把UID点的值广播出去
    }
  }

  def mergeMsg_UID(msg1: String, msg2: String): String = {
    //取第一个
    msg1
  }

  def vprog_UID(vertexId: VertexId, value: (String, (Long, String)), message: String): (String, (Long, String)) = {
    if (message.compareTo("UID") == 0) {
      // println(value._1 + " is assigned vertex id " + vertexId + " , initialMsg is " + initialMsg + " , message is " + message )
      (value._1, (vertexId, "")) //最初把每个节点的值都初始为自己的节点序号
    } else {
      //println(value._1 + " is assigned " + (message min value._2) + " , message is " + message )
      (value._1, (vertexId, message))
    }
  }

  def apply(graph: Graph[(String, (Long, String)), (String, Long)]) = {
    //找出图中所有节点关联的UID节点,并记录到节点的UID属性中
    graph.pregel(
      "UID", //初始消息,设置起点 
      2, //2 层就够了
      EdgeDirection.Out)(
        vprog_UID,
        sendMsg_UID,
        mergeMsg_UID)
  }
}