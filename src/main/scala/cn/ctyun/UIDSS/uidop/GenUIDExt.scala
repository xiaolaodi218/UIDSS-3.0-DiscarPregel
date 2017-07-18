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
 * 5. 计算出号码间距离 （QQ, id, CI按不同权重），关联同一用户的号码，得到该用户列表。
 * 6. 更新所有用户节点的UID （QQ有UID，移动号码原来有，固网号码，都没有生成）
 * 7. 记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）, 保存到数据库
 */
object GenUIDExt extends Logging{

  val initialMsg = GraphXUtil.MAX_VERTICE
  var UID_PRIOR_QQ = 2
  var UID_PRIOR_ID = 1
  var UID_PRIOR_CI = 1
  var UID_PRIOR_AN = 1
  var UID_PRIOR_WN = 1
  var UID_PRIOR_MN = 2
  
  def getUID(group: List[(String, String)]): Iterable[(String, List[(String, String)])] = {

    //println("********************  GenUIDExt.getUID ***************************")
    val uidGraph = UIDGraph(group)

    if (uidGraph.getSize() > 0) {
      if (uidGraph.getSize() > 1000) {
         println(getNowDate() + " ****** get graph has " +  uidGraph.getSize()  +" nodes. Bigest nodes is " + uidGraph.getBigestNode()  +"  ****** "  )   
         ListBuffer[(String, List[(String, String)])]()
      }
      else {
      //生成连接子图，也就是独立用户
      //以号码为中心，拆开连通图，形成号码级用户
      //按（QQ，TDID）等ID信息合并号码级用户
      //得到合并后的用户（独立用户）, 包括1个或多个号码级用户合并的结果
      //找出或生成UID（QQ，TDID>号码 > 新生成）
      //uidGraph.searchUsers()

      //返回UID，最后写回HBase
      //只要保存UID与QQ,TDID和号码等的关联就行. 
      //因为所有信息是以号码为中心保存的,按UID查询时, 只要按照号码查就行.
      //而qq等保存UID，是为了保持稳定性.
      //(新UID： String, List[(节点：String, 原UID： String)])   
      uidGraph.getUID()
      }
    } else {
      //println("********************  GenUIDExt.getUID:  No elements in this graph. ********************")
      ListBuffer[(String, List[(String, String)])]()
    }

  }

  def apply(rddCnndGroup: RDD[List[(String, String)]], props: Properties): RDD[(String, List[(String, String)])] = {

    if (props.getProperty("UID_PRIOR_QQ").length() > 0) UID_PRIOR_QQ = props.getProperty("UID_PRIOR_QQ").toInt
    if (props.getProperty("UID_PRIOR_ID").length() > 0) UID_PRIOR_ID = props.getProperty("UID_PRIOR_ID").toInt
    if (props.getProperty("UID_PRIOR_CI").length() > 0) UID_PRIOR_CI = props.getProperty("UID_PRIOR_CI").toInt
    if (props.getProperty("UID_PRIOR_AN").length() > 0) UID_PRIOR_AN = props.getProperty("UID_PRIOR_AN").toInt
    if (props.getProperty("UID_PRIOR_MN").length() > 0) UID_PRIOR_MN = props.getProperty("UID_PRIOR_MN").toInt
    if (props.getProperty("UID_PRIOR_WN").length() > 0) UID_PRIOR_WN = props.getProperty("UID_PRIOR_WN").toInt

    //UID生成
    //算出优势UID，如果没有则要生成
    //println("************Before getUID(), there is/are  " + rddCnndGroup.count() + " graphs.***********")
    //println("rddCnndGroup is " + rddCnndGroup.collect().mkString("\n"))
    println(getNowDate() + " ****** getUID()   ******")   
    
//    val rddPrint = rddCnndGroup.flatMap {
//      case (group) => {
//        //println("group is tested " + group.mkString("\n"));
//        List(0, 0, 1)
//      }
//    }
    //println("rddPrint is " + rddPrint.count())

    val rddVtoVwithSN = rddCnndGroup.flatMap { case (group) => getUID(group) }

    //println("************After getUID(), there is/are " + rddVtoVwithSN.count() + " seperated graphs. *************")
    //println("rddVtoVwithSN is " + rddVtoVwithSN.collect().mkString("\n"))
    rddVtoVwithSN
  }
}