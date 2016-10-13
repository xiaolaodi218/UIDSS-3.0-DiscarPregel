package cn.ctyun.UIDSS.graphxop

import org.apache.spark.graphx._
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph._

//找出图中所有节点关联的UID节点,并记录到节点的UID属性中
object PregelGenUIDExtFindLinks {

  //广播找出关联的所有节点
  def sendMsg_UID(triplet: EdgeTriplet[(String, (Long, String)), (String, Long)]): Iterator[(VertexId, String)] = {
    val typ = triplet.srcAttr._1.substring(0, 2)
    if (triplet.srcAttr._2._1 == triplet.dstId ) {
      //println(triplet.srcAttr._2 + " is terminated " )    
      Iterator.empty //如果当前节点值==目标节点, 不用再遍历
    } else {
      //println( triplet.dstId  + " is sent message  " + triplet.srcAttr._2 )
      Iterator((triplet.dstId, triplet.srcAttr._1)) //把UID点的值广播出去
    }
  }

  def mergeMsg_UID(msg1: String, msg2: String): String = {
    //保存所有的
    //println("Send message:  " + msg1 +"," + msg2 )
    msg1 +";" + msg2
  }

  def vprog_UID(vertexId: VertexId, value: (String, (Long, String)), message: String): (String, (Long, String)) = {
    if (message.compareTo("UID") == 0) {
      //println(value._1 + " is assigned vertex id " + vertexId + " , message is " + message )
      (value._1, (vertexId, "")) //最初把每个节点的值都初始为自己的节点序号
    } else {
      //println(value._1 + " is assigned vertex id " + vertexId + " , message is " + message )
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