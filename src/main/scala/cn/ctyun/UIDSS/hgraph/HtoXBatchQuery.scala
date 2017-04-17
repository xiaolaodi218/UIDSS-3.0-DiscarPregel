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
package cn.ctyun.UIDSS.hgraph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes;

import scala.collection.mutable.ListBuffer

object HtoXBatchQuery {

  //把HBase一行转换成多条边， 
  //（目标节点id, (源节点sn, 连接类型)）
  //可以在这一步进行边的过滤，搜索时直接计算连接关系即可
  def convertForNormalSearch(sn: Long, v: Result): Iterable[(String, (Long, String))] = {
    val row = Bytes.toString(v.getRow.drop(2))
    val buf = new ListBuffer[(String, (Long, String))]
    for (c <- v.rawCells()) {
      var dst = Bytes.toString(c.getQualifier)
      val ty = dst.substring(0, 2)
      dst = dst.substring(2)
      var value = 0
      try {
        value = Bytes.toInt(c.getValue)
      } catch {
        case _: Throwable =>
      }
      if (value > 0)
        buf += ((dst, (sn, ty)))
    }
    buf.toIterable
  }

  def getGraphRDD(rdd: RDD[(ImmutableBytesWritable, Result)]): Graph[(String, Long), (String, Long)] = {

    //-------------------------------------------------
    //点序号
    //-------------------------------------------------
    //生成序号
    //为每个起始点加序列号sn，也就是HBase分区序号*每分区最大行数 + 该分区内的行序号。
    //比如： （100100001, {AI430851876/v:$1AN06F6642CE07804C26B847BEAEEB0204A/1458733523410/Put/vlen=4/mvcc=0} ) 
    val rddHBaseWithSN = rdd
      .mapPartitionsWithIndex { (ind, vs) =>
        var sn = GraphXUtil.MAX_VERTICE + ind * GraphXUtil.MAX_VERTICE_PER_PARTITION
        vs.map {
          case (_, v) =>
            sn = sn + 1
            (sn, v)
        }
      }

    //点id到点序号对应关系
    //（点id，(点序号,"sn")） 
    // 比如： （AI430851876，（100100001,"sn"））
    val rddVIdtoSN = rddHBaseWithSN.map[(String, (Long, String))]({ case (sn, v) => (Bytes.toString(v.getRow.drop(2)), (sn, "sn")) })

    //-------------------------------------------------
    //点集合
    //-------------------------------------------------
    //点集合第1步
    //Vertex (vid: VertextId: Long, (id: String, temp:Long ))
    val rddVertex = rddVIdtoSN.map({ case (id, (sn, _)) => (sn, (id, 0L)) } )  
    //println(rddVertex.collect().mkString("\n"))

    //-------------------------------------------------
    //边集合  
    //-------------------------------------------------
    //边集合第1步 - 点到点关系，同时替换起点序号
    //(终止点id, (起始点序号，边属性）)
    //比如：(AI430851876, (100100001，"$1"))    
    val rddVtoV = rddHBaseWithSN.flatMap[(String, (Long, String))]({ case (sn, v) => convertForNormalSearch(sn, v) })
    //rddVtoV.foreach { case (dst, (l, prop)) => println("Dst node id is: " + dst +";  source node serial number is: " + l + "; prop is:  " + prop) }

    //边集合第2步 - 通过join实现终点id与序号对应
    //(v, ((sn, prop), (snsn, snty))
   val rddVtoVwithSN = rddVtoV.join(rddVIdtoSN)
   //println(rddVtoVwithSN.collect().mkString("\n"))
   
   //边集合第3步 - 替换终点序号，生成边的triplet
   //Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
   val rddEdge = rddVtoVwithSN.map({ case (vid, ((sn, prop), (snsn, snty))) => Edge(snsn, sn, (prop, 1L) )})
   //println(rddEdge.collect().mkString("\n"))
   
   //-------------------------------------------------
   //生成图
   //-------------------------------------------------
   Graph(rddVertex, rddEdge, null )
  }
}