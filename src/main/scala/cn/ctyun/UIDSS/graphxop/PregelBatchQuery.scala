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
package cn.ctyun.UIDSS.graphxop

import org.apache.spark.graphx._
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

object PregelBatchQuery {

  val initialMsg = 2*GraphXUtil.MAX_VERTICE
  //triplet: EdgeTriplet[VD, ED]
  def sendMsg(triplet: EdgeTriplet[(String, Long), (String, Long)]): Iterator[(VertexId, Long)] = {

    if (triplet.srcAttr._2 == triplet.dstId) {
      //println(triplet.srcAttr._2 + " is terminated " )    
      Iterator.empty //如果当前节点值==目标节点, 不用再遍历
    } else {
      val nodeType = triplet.dstAttr._1.substring(0, 2)
      val iNodeType = Bytes.toShort(Bytes.toBytes(nodeType))
      if (iNodeType == HGraphUtil.CLMN_PROD_INST
        || iNodeType == HGraphUtil.CLMN_IMSI
        || iNodeType == HGraphUtil.CLMN_IMEI
        || iNodeType == HGraphUtil.CLMN_MEID
        || iNodeType == HGraphUtil.CLMN_ESN
        || iNodeType == HGraphUtil.CLMN_ACCT_ID
        || iNodeType == HGraphUtil.CLMN_ICCID) {
        Iterator.empty //按节点类型过滤。叶子节点不再去遍历。
      } else {
        //println( triplet.dstId  + " is sent message  " + triplet.srcAttr._2 )  
        Iterator((triplet.dstId, triplet.srcAttr._2)) //把源节点的值传递下去
      }
    }
  }

  def mergeMsg(msg1: Long, msg2: Long): Long = {
    //Only by selecting the smallest one can all the connected vertex ultimately be grouped into one group. 
    msg1 min msg2
  }

  def vprog(vertexId: VertexId, value: (String, Long), message: Long): (String, Long) = {
    if (message == initialMsg) {
      // println(value._1 + " is assigned vertex id " + vertexId + " , initialMsg is " + initialMsg + " , message is " + message )
      (value._1, vertexId) //最初把每个节点的值都初始为自己的节点序号
    } else {
      //println(value._1 + " is assigned " + (message min value._2) + " , message is " + message )
      (value._1, message min value._2)
    }
  }
  
  def apply(graph: Graph[(String, Long), (String, Long)] )  ={
     //找出所有节点关联的树,树名为树中节点的最小序号
     //自带的类似功能  graph.connectedComponents()
      graph.pregel(
                                initialMsg,           //初始消息,设置起点 
                                5,                                            //5 层就够了
                                EdgeDirection.Out
                            )(
                                vprog,
                                sendMsg,
                                mergeMsg
                            )
  }
}