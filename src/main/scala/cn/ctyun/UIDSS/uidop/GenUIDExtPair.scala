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

package cn.ctyun.UIDSS.uidop

import java.util.Properties
import java.util.UUID
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil }
import cn.ctyun.UIDSS.utils.{ Utils, Logging }

/**
 * 类描述：生成UID操作
 * 1. 生成图时, 需要把关联的节点写入节点属性
 * 2. 遍历生成关联树后,
 * 3. 找出所有号码
 * 4. 号码间所有关系（QQ, id, CI）
 * 5. 计算出号码间距离 （QQ, id, CI按不同权重），关联同一用户的号码
 */
object GenUIDExtPair extends Logging{
  
  def findTruePair(group: List[(String, String)])= {

    //号码对之间的小图
    val uidPairGraph = UIDPairGraph(group)

    if (uidPairGraph.getSize() > 0) {
      //生成连接子图，也就是独立用户
      //以号码为中心，拆开连通图，形成号码级用户
      //按（QQ，TDID）等ID信息合并号码级用户
      uidPairGraph.belongToSameUser()
    } else {
      false
    }
  }

  def expandPairToGraph(pair: List[(String, String)]): List[(String, String)] = {

    var g = Map[String, Set[String]]()
    for (v <- pair) {
      //把两端的节点加入图
      val fields = v._2.split(";")
      g += (v._1 -> fields.toSet)

      //把相邻节点逐个加入图
      if (fields.size > 0) {
        for (field <- fields) {
          if (g.contains(field)) {
            var neighbors = g.get(field).get
            neighbors += v._1
            g -= field
            g += (field -> neighbors)
          } else {
            var neighbors = Set[String]()
            neighbors += v._1
            g += (field -> neighbors)
          }
        }
      }
    }
    var r = ListBuffer[(String, String)]()
    for (v <- g) {
      var ns = ""
      for (n <- v._2) {
        ns = ns + ";" + n
      }
      ns = ns.substring(1)
      r += ((v._1, ns))
    }
    r.toList
  }

  def apply(rddCnndGroup: RDD[(((Long, String), String), ((Long, String), String))], props: Properties): RDD[((Long, String), (Long, String))] = {

    // (((s_vid,s_id),s_links),((e_vid,e_id), e_links))
    val rddPairs = rddCnndGroup.flatMap {
      case (((s_vid, s_id), s_links), ((e_vid, e_id), e_links)) => {
        if (s_links.length() > 0 && e_links.length() > 0) {
          var pairGraph = expandPairToGraph(List((s_id, s_links), (e_id, e_links)))
          if (findTruePair(pairGraph)) {
            List(((s_vid, s_id), (e_vid, e_id)))
          } else {
            List(((s_vid, s_id), (0L, "")), ((e_vid, e_id), (0L, "")))
          }
        } else {
          List(((s_vid, s_id), (0L, "")), ((e_vid, e_id), (0L, "")))
        }
      }
    }

    rddPairs
  }
}