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

import java.util.Properties

import org.apache.spark.graphx.{EdgeTriplet,VertexId,Graph,EdgeDirection}
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

import scala.collection.mutable.ListBuffer


//找出图中所有的关联树,树名为树中节点的最小序号
object PregelGenUIDFindPairs {

  val initialMsg: Long = 2*GraphXUtil.MAX_VERTICE

  //找出连接号码对
  //Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
  //Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
  def sendMsg(edge: EdgeTriplet[(String, (Long, List[(Long, Long)])), (String, Int)]): Iterator[(VertexId, List[(Long, Long)])] = {

    val srcID = edge.srcId
    val dstID = edge.dstId
    val srcNeighbors = edge.srcAttr._2._2
    val dstNeighbors = edge.dstAttr._2._2

    var dstNewNeighbors = new ListBuffer[(Long, Long)]
    var srcNewNeighbors = new ListBuffer[(Long, Long)]

    //比较两端是否有对端不知道的邻居
    for (srcNeighbor <- srcNeighbors) {
      val matching = dstNeighbors.find({ _._1 == srcNeighbor._1 })
      if (matching == None) {
        //尚未发现的邻居
        dstNewNeighbors += ((srcNeighbor._1, srcID))
      }
    }
    for (dstNeighbor <- dstNeighbors) {
      val matching = srcNeighbors.find({ _._1 == dstNeighbor._1 })
      if (matching == None) {
        //尚未发现的邻居
        srcNewNeighbors += ((dstNeighbor._1, dstID))
      }
    }

    //把彼此不知道节点的传递下去
    if (dstNewNeighbors.size > 0 && srcNewNeighbors.size > 0) {
      Iterator((dstID, dstNewNeighbors.toList), (srcID, srcNewNeighbors.toList))
    } 
    else if (dstNewNeighbors.size > 0 && srcNewNeighbors.size == 0) {
      Iterator((dstID, dstNewNeighbors.toList))
    } 
    else if (dstNewNeighbors.size == 0 && srcNewNeighbors.size > 0) {
      Iterator((srcID, srcNewNeighbors.toList))
    } 
    else {
      Iterator.empty //两边都没有需要广播的新邻居
    }
  }

  def mergeMsg(msg1: List[(Long, Long)], msg2: List[(Long, Long)]): List[(Long, Long)] = {
    //Only by selecting the smallest one can all the connected vertex ultimately be grouped into one group. 
    //只是把消息连接起来,不做其它处理
    var msgs = msg1:::msg2    
    msgs = msgs.sortWith(_._1<_._1)
    var msgBuf = new ListBuffer[(Long, Long)]
    var cur = 0L
    for (msg  <-  msgs) {
      if (msg._1 > cur){ 
        msgBuf+= ((msg))
        cur = msg._1
      }
    }
    msgBuf.toList
  }

  def vprog(vertexId: VertexId, value: (String, (Long, List[(Long, Long)])), message: List[(Long, Long)]): (String, (Long, List[(Long, Long)])) = {

    if (1 == message.length && initialMsg == message.head._1) { //初始化消息
      var neighbors = new ListBuffer[(Long, Long)]
      if (HGraphUtil.STR_ACCS_NUM.compareTo(value._1) == 0
          || HGraphUtil.STR_MBL_NUM.compareTo(value._1) == 0
          || HGraphUtil.STR_WB_NUM.compareTo(value._1) == 0) { //通信号码节点
        neighbors +=((vertexId ,0L))
      } 
      //初始化时，保留初始值不变。 value._1 为节点类型；相邻最小值都初始为0（在本功能无用处）； 
      //通信号码节点的邻接号码列表有一个元素，即自己到自己，其它节点没有
      (value._1, (0, neighbors.toList)) 
    } else {
      //中间pregel消息
        var neighbors =  value._2._2
        var newNeighbors = new ListBuffer[(Long, Long)]
        //把新路径更新进列表
        for (msg <- message) {
                  var neighbor = neighbors.find({_._1==msg._1})
                  if (neighbor == None) {
                    //如果没有就添加
                    newNeighbors += ((msg._1   ,msg._2))                    
                  }             
        }
        //把原来的路径也写进列表
        for (neighbor <- neighbors) {
                  val msgMatching = message.find({_._1==neighbor._1})
                  if (msgMatching == None) {
                    newNeighbors += ((neighbor._1   ,neighbor._2))                    
                  }             
        }
        
        (value._1, (0, newNeighbors.toList)) //记下最小的节点序号   
    }
  }

  def apply(graph: Graph[(String, (Long, List[(Long, Long)])), (String, Int)], depth: Int, props: Properties ) = {
    //找出图中所有节点关联的UID节点,并记录到节点的UID属性中
    Pregel(graph,
      List((initialMsg, 0L)), //初始消息,设置起点 
      depth, // 考虑网络侧的关联,多少层在配置文件里确定
      EdgeDirection.Either, props)(
        vprog,
        sendMsg,
        mergeMsg)
//    graph.pregel(
//      List((initialMsg, 0L)), //初始消息,设置起点 
//      depth, // 考虑网络侧的关联,多少层在配置文件里确定
//      EdgeDirection.Either)(
//        vprog,
//        sendMsg,
//        mergeMsg)
  }
}