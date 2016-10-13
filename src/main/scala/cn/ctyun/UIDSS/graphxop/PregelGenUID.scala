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

package cn.ctyun.UIDSS.graphxop

import org.apache.spark.graphx._
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

//找出图中所有的关联树,树名为树中节点的最小序号
object PregelGenUID {

  val initialMsg = GraphXUtil.MAX_VERTICE

  //找出连接树
  //Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
  //Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
  def sendMsg(edge: EdgeTriplet[(String, Long), (String, Int)]): Iterator[(VertexId, Long)] = {
    
    //println("sendMsg;  " +  edge.toString() )
    
    if (edge.srcAttr._2 == edge.dstAttr._2) {
      //println(triplet.srcAttr._2 + " is terminated " )    
      Iterator.empty //如果当前节点所属树的序号==目标节点所属树的序号, 不用再遍历
    } else {
      var nodeType = ""
      var dst = 0L
      var src = 0L
      var v = 0L

      if (edge.srcAttr._2 < edge.dstAttr._2) {
        val nodeType = edge.srcAttr._1.substring(0, 2)
        dst = edge.dstId
        src = edge.srcId
        v = edge.srcAttr._2
      } else if (edge.srcAttr._2 > edge.dstAttr._2) {
        val nodeType = edge.dstAttr._1.substring(0, 2)
        dst = edge.srcId
        src = edge.dstId
        v = edge.dstAttr._2
      }

      //需要按UID连接, 以便删除旧的UID
      //if (HGraphUtil.STR_UD.compareToIgnoreCase(nodeType)==0  ) {
      //  Iterator.empty //按节点类型过滤。UID只接收(已经在vprog处理了),不再往下传递 
      //} else {
        if (edge.srcAttr._1.length()>2 || edge.dstAttr._1.length()>2) {
          println( "" + src  + " is sending message  " + v + "  to  vertex " + dst  + " " )
          println( "src is " + edge.srcAttr.toString()  + "; dst is " +edge.dstAttr.toString() )
        }  
        Iterator((dst, v)) //把源节点的值传递下去
      //}
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
      (value._1, message min value._2) //记下最小的节点序号
    }
  }

  def apply(graph: Graph[(String, Long), (String, Int)], depth: Int) = {
    //找出图中所有节点关联的UID节点,并记录到节点的UID属性中
    graph.pregel(
      initialMsg, //初始消息,设置起点 
      depth, // 考虑网络侧的关联,多少层在配置文件里确定
      EdgeDirection.Either)(
        vprog,
        sendMsg,
        mergeMsg)
  }
}