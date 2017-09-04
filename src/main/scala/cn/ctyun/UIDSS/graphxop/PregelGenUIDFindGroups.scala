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

import java.util.Properties

import org.apache.spark.graphx.{EdgeTriplet,VertexId,Graph,EdgeDirection}
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

import scala.collection.mutable.ListBuffer


//找出图中所有的关联树,树名为树中节点的最小序号
object PregelGenUIDFindGroups {

  val initialMsg: Long = 2*GraphXUtil.MAX_VERTICE

  //找出连接子图
  //Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
  //Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
  def sendMsg(edge: EdgeTriplet[(String, (Long, List[(Long, Long)])), (String, Int)]): Iterator[(VertexId, List[(Long, Long)])] = {
    
    //println("sendMsg;  " +  edge.toString() )
    val srcType : String = edge.srcAttr._1.substring(0, 2)  
    val dstType : String = edge.dstAttr._1.substring(0, 2) 
    var srcMinID = edge.srcAttr._2._1
    var dstMinID = edge.dstAttr._2._1
    val srcNeighbors =  edge.srcAttr._2._2
    val dstNeighbors =  edge.dstAttr._2._2
    
    //分情况取出边的两端的最小ID
    //其中：宽带与宽带之间直接相连的情况已经在前面过滤掉.
    if (0== HGraphUtil.STR_WB_NUM.compareTo(srcType) || 0== HGraphUtil.STR_ACCS_NUM.compareTo(srcType)) { //src是宽带节点
        val neighbor = srcNeighbors.find({_._1==edge.dstId})
        if (neighbor != None) {
          srcMinID = neighbor.get._2
        }
    }
    else if (0== HGraphUtil.STR_WB_NUM.compareTo(dstType)|| 0== HGraphUtil.STR_ACCS_NUM.compareTo(dstType)) { //dst是宽带节点
        val neighbor = dstNeighbors.find({_._1==edge.srcId})
        if (neighbor != None) {
          dstMinID = neighbor.get._2
        }
    }
    else {  //两端都是普通节点      
    }
    
    //边的两端的最小ID的大小相等，不需要传播什么
    if (srcMinID == dstMinID) {
      //println(triplet.srcAttr._2 + " is terminated " )    
      Iterator.empty //如果当前节点所属树的序号==目标节点所属树的序号, 不用再遍历
    } else {
      
      var dst = 0L
      var src = 0L
      var v = 0L

    //分情况取出边的两端的最小ID
    //其中：宽带与宽带之间直接相连的情况已经在前面过滤掉.
    //宽带号不应该广播自己。其它节点一直往宽带号广播。直到对应ID相等。
    if (0== HGraphUtil.STR_WB_NUM.compareTo(srcType) || 0== HGraphUtil.STR_ACCS_NUM.compareTo(srcType)) { //src是宽带节点
        dst = edge.srcId
        src = edge.dstId
        v = dstMinID
    }
    else if (0== HGraphUtil.STR_WB_NUM.compareTo(dstType) || 0== HGraphUtil.STR_ACCS_NUM.compareTo(dstType)) { //dst是宽带节点
        dst = edge.dstId
        src = edge.srcId
        v = srcMinID
    }
    else {  //两端都是普通节点      
      //根据边的两端的最小ID的大小确定传播方向和值
      if (srcMinID < dstMinID) {
        dst = edge.dstId
        src = edge.srcId
        v = srcMinID
      } 
      else if (srcMinID > dstMinID) {
        dst = edge.srcId
        src = edge.dstId
        v = dstMinID
      }
    }
      
      //调试代码
        if (edge.srcAttr._1.length()>2 || edge.dstAttr._1.length()>2) {
          println( "" + src  + " is sending message  " + v + "  to  vertex " + dst  + " " )
          println( "src is " + edge.srcAttr.toString()  + "; dst is " +edge.dstAttr.toString() )
        }  
        Iterator((dst, List((src,v)))) //把有较小ID的节点的值传递下去
      //}
    }
  }

  def mergeMsg(msg1: List[(Long, Long)], msg2: List[(Long, Long)]): List[(Long, Long)] = {
    //Only by selecting the smallest one can all the connected vertex ultimately be grouped into one group. 
    //只是把消息连接起来,不做其它处理
    msg1:::msg2
  }

  def vprog(vertexId: VertexId, value: (String, (Long, List[(Long, Long)])), message: List[(Long, Long)]): (String, (Long, List[(Long, Long)])) = {

    if (1 == message.length && initialMsg == message.head._1) { //初始化消息
      // println(value._1 + " is assigned vertex id " + vertexId + " , initialMsg is " + initialMsg + " , message is " + message )
      (value._1, (vertexId, value._2._2)) //最初把每个节点的值都初始为自己的节点序号
    } else {
      
      //普通pregel消息
      var minimal: Long = initialMsg
      for (msg <- message) {
        if (msg._2 < minimal) { minimal = msg._2 }
      }
      
      if (HGraphUtil.STR_WB_NUM.compareTo(value._1) != 0 && HGraphUtil.STR_ACCS_NUM.compareTo(value._1) != 0) { //非宽带节点
        //println(value._1 + " is assigned " + (message min value._2) + " , message is " + message )
        (value._1, (value._2._1 min minimal, value._2._2)) //记下最小的节点序号         
      } 
      else { //宽带节点
        var neighbors =  value._2._2
        var newNeighbors = new ListBuffer[(Long, Long)]
        //把新消息更新进列表
        for (msg <- message) {
                  var neighbor = neighbors.find({_._1==msg._1})
                  if (neighbor != None) {
                    //替换邻居最小值
                    newNeighbors +=((msg._1   ,msg._2 min neighbor.get._2))
                  } 
                  else {
                    //如果没有就添加
                    newNeighbors += ((msg._1   ,msg._2))                    
                  }             
        }
        //把没有得到消息的邻居也写进列表
        for (neighbor <- neighbors) {
                  val msgMatching = message.find({_._1==neighbor._1})
                  if (msgMatching == None) {
                    //没有得到消息的邻居
                    newNeighbors += ((neighbor._1   ,neighbor._2))                    
                  }             
        }
        
        (value._1, (value._2._1 min minimal, newNeighbors.toList)) //记下最小的节点序号   
      }
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