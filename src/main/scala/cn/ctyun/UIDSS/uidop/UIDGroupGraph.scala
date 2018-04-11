/**
 * *******************************************************************
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
 * ********************************************************************
 */

package cn.ctyun.UIDSS.uidop

import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil }
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

object UIDGroupGraph {

  def apply(group: List[(String, String)]) = {
    //println("********************  UIDGraph.apply ***************************")
    val graph = new UIDGroupGraph(group)
    graph.init()
    graph
  }
}

/*  用户ID生成
*/
class UIDGroupGraph(group: List[(String, String)]) {
  //所有节点放到图(即邻接数组)中
  private var g = Map[String, Set[String]]()
  //所有移网号码,固网号码,宽带号码
  private var ans = List[String]()
  //找到的号码间相等关系图
  private var gEqu = Map[String, Set[String]]()

  def getSize(): Int = {
    g.size
  }  
  
  /*  init :  初始化图，并找出号码
 *  		1. 所有节点放到图(即邻接数组)中 g
 *  		2.以号码为中心，拆开连通图，形成号码级用户 ans
 *  		3.按（QQ，TDID）等ID信息连接号码级用户，并保存在 gEqu
*/
  private def init() = {

    var vlist = Set[String]()
    for (v <- group) {
       vlist += v._1
    }
    
    for (v <- group) {
      //1.所有节点放到图(即邻接数组)中
      
      val fields = v._2.split(";")
      if (v._2.length() > 0 && fields.size > 0) {
        var neighbors = Set[String]()
        //只有在图中的点的邻接关系被考虑,WN上连的UID不会通过WN连上,而是通过QQ等连上
        for (field <- fields) {
          if (vlist.contains(field)) {neighbors +=field}
        }
        //neighbors ++= fields.toList
        g += (v._1 -> neighbors)
        //println(v._1 + " has neighbors: " + neighbors.toList.toString())

        val typ = v._1.substring(0, 2)
        typ match {
          case HGraphUtil.STR_ACCS_NUM => {
            //2.找到所有移网号码,固网号码,宽带号码列表
            ans = ans ::: List(v._1)

            //3.用来保存结果的号码间相等关系图 
            gEqu += (v._1 -> Set[String]())
          }
          case HGraphUtil.STR_MBL_NUM => {
            //只计算电信号码, 不计算异网移动号码UID
            val isCTC = neighbors.exists { _.substring(0, 2).compareToIgnoreCase(HGraphUtil.STR_IMSI) == 0 }
            if (isCTC) {
              //2.找到所有移网号码,固网号码,宽带号码列表
              ans = ans ::: List(v._1)
              //3.用来保存结果的号码间相等关系图 
              gEqu += (v._1 -> Set[String]())
            }
          }
          case HGraphUtil.STR_WB_NUM => {
            //2.找到所有移网号码,固网号码,宽带号码列表
            ans = ans ::: List(v._1)

            //3.用来保存结果的号码间相等关系图 
            gEqu += (v._1 -> Set[String]())
          }
          case _ =>
        }
      }

    }

  }

  /*getUID： 得到UID边列表 
   * 三次全图遍历：
   * 		1.按（QQ，TDID）等ID信息，连接同一用户的号码，并保存在 gEqu
   *   	2.把同一用户的号码放在同一组，也就是独立用户( 包括1个或多个号码级用户合并的结果)
   *    3.找出或生成用户号码组的UID（QQ，TDID>号码 > 新生成）
   * 
   * 返回：UID边列表 (新UID： String, List[(节点：String, 原UID： String)])，以后可以写回HBase
   *  - 只要保存UID与QQ,TDID和号码等的关联就行. 
   *  - 因为所有信息是以号码为中心保存的,按UID查询时, 只要按照号码查就行；而qq等保存UID，是为了保持稳定性.
    */
  def getUID(): Iterable[(String, List[(String, String)])] = {

    //println("******************** UIDGraph.getUID ************************")

    //3.找出或生成用户号码组的UID（QQ，TDID>号码 > 新生成）
    var vVisitied = Set[String]()
    var uidsUsed = Set[String]()
    var group = ans.toSet
    val lstEdgeWithUid = getPreferUID(vVisitied, uidsUsed)
    List(lstEdgeWithUid) 
  }  

  /*3.找出或生成用户号码组的UID（QQ，TDID>号码 > 新生成）
   * @groupset	 用户的所有电话号码		
   * @return  返回 UID边列表 (新UID： String, List[(节点：String, 原UID： String)])，以后可以写回HBase
  */
  private def getPreferUID(vVisitied: Set[String], uidsUsed: Set[String]): (String, List[(String, String)]) = {

    var needPrint = false
    //返回与号码组相邻的所有ID节点(也包括号码节点自身), 和对应UID。
    //是简化的2层dfs遍历
    val lstbufUIDs = new ListBuffer[(String, String)]
    for (an <- ans) {
      val lnks = g.getOrElse(an, Set[String]())
      var uidAN = ""
      //与号码相邻的所有节点
      for (vID <- lnks) {
        val typ = vID.substring(0, 2)
        typ match {
          //是UID节点，记为号码节点的UID
          case HGraphUtil.STR_UD => {
            uidAN = vID
          }
          case _ =>
        }
      }
      //号码节点直连的ＵＩＤ, 所有标识节点都应该保存在lstbufUIDs,哪怕是空的.
      if (!vVisitied.contains(an)) {
        vVisitied += an
        lstbufUIDs += ((an, uidAN))
      }
    }
    //以上步骤为了找到ans(此ans只会有孤立号码，和成对号码，如：AN123   WN123;MN123)中的每个号码的原有UID邻接点，output：list((WM123,""),(MN123,UD123))
    var lstGroupUID: List[(String, String)] = lstbufUIDs.toList

    //算出每个UID的加权值
    var uidCounts = Map[String, Int]()
    for (vertex <- lstGroupUID) {
      //没有UID的节点(是为了以后补上UID加的)不参加优势UID
      //有UID邻接点的号码才会走这段，原来没有UID邻接点的不走这段
      if (vertex._2.length() > 0) {
        var count = uidCounts.getOrElse(vertex._2, 0)
        //与不同类型节点相连的UID权重是不同的
        val typ = vertex._1.substring(0, 2)
        typ match {
          case HGraphUtil.STR_MBL_NUM  => count += (GenUIDExt.UID_PRIOR_MN)
          case HGraphUtil.STR_ACCS_NUM => count += (GenUIDExt.UID_PRIOR_AN)
          case HGraphUtil.STR_WB_NUM   => count += (GenUIDExt.UID_PRIOR_WN)
          case _                       => count = count
        }
        uidCounts += (vertex._2 -> count)
      }
    }

    //出现次数最多的UID被选为有效UID
    //实际上权值最大的为有效UID
    var maxcount = 0
    var maxUid = ""
    uidCounts.foreach {
      case (rowkey, cnt) => {
        if (cnt > maxcount) {
          maxcount = cnt
          maxUid = rowkey
        }
      }
    }

    //别的组已经用过(抢先分得)的原有UID不能再用
    if (maxUid.length() > 0 && uidsUsed.contains(maxUid)) {
      maxUid = ""
    }

    if (maxUid.length() == 0) {
      maxUid = HGraphUtil.STR_UD + generateUID()
    }

    uidsUsed += maxUid

    lstGroupUID = lstGroupUID.map { vertex =>
      {
        //宽带有多个UID, 只增不删,不能覆盖
        //如果MN原先有UD则用原来的UID，如果WN原来也有UID则用优势的UID，如果两个都没有则不变，还是((WN123,""),(MN123,""))
        if (vertex._2.length() > 0) {
          var count = uidCounts.getOrElse(vertex._2, 0)
          val typ = vertex._1.substring(0, 2)
          typ match {
            case HGraphUtil.STR_WB_NUM => (vertex._1, maxUid)
            case _                     => (vertex._1, vertex._2)
          }
        } else {
          (vertex._1, vertex._2)
        }
      }
    }
    //如果两个号码都没有UID则会新生成一个UID为maxUid
    //孤立节点也会生成一个新的UID为maxUid
    (maxUid, lstGroupUID)
  }

  //生成新的唯一的UID号
  private def generateUID(): String = {
    UUID.randomUUID().toString().replace("-", "")
  }

}