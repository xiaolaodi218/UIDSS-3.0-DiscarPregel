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

  def apply(rddCnndGroup: RDD[(((Long, String), String), ((Long, String), String))], props: Properties): RDD[(((Long, String), String), ((Long, String), String))]= {
    
    // (((s_vid,s_id),s_links),((e_vid,e_id), e_links))
    val rddVtoVwithSN = rddCnndGroup.flatMap { case (((s_vid,s_id),s_links),((e_vid,e_id), e_links)) => {
       var pair= List((s_id, s_links), (e_id, e_links))
       if ( findTruePair(pair)) {
         List((((s_vid,s_id),s_links),((e_vid,e_id), e_links)))
       } else {
         List((((s_vid,s_id),s_links),((0L,""), "")),(((e_vid,e_id),e_links),((0L,""), "")))
       }
      } 
    }

    //println("************After getUID(), there is/are " + rddVtoVwithSN.count() + " seperated graphs. *************")
    //println("rddVtoVwithSN is " + rddVtoVwithSN.collect().mkString("\n"))
    rddVtoVwithSN
  }
}