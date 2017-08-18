/**
 * *******************************************************************
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
 * ********************************************************************
 */

package cn.ctyun.UIDSS.uidop

import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil }
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

object UIDPairGraph {

  def apply(group: List[(String, String)]) = {
    //println("********************  UIDGraph.apply ***************************")
    val graph = new UIDPairGraph(group)
    graph.init()
    graph
  }

}

/*  用户ID生成
*/
class UIDPairGraph(group: List[(String, String)]) {
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
    var needPrint = false

    var vlist = Set[String]()
    for (v <- group) {
       vlist += v._1
    }
    
    for (v <- group) {
      //      if ("QQ974834277".compareToIgnoreCase(v._1) == 0) {
      //        needPrint = true
      //      }
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

        if (v._1.length()<2) {
          println(v._1 + " has neighbors: " + neighbors.toList.toString())
          println( " graph is : " + g.toList.toString())
          println( " group is : " + group.toList.toString())  
        } 
        
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
    if (needPrint) {
      println("**********graph  is " + g.mkString("|"))
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
  def belongToSameUser(): Boolean = {

    //println("******************** UIDGraph.getUID ************************")

    //0.找出超级ID单独放在表中

    //1.按（QQ，TDID）等ID信息，连接同一用户的号码，并保存在 gEqu
    findEqualANPair()
    //println("findEqualANPair(), map gEqu: " + gEqu.toString())
    
    var iEqu = 0
    for (itr <-gEqu) {
      iEqu =  iEqu + itr._2.size
    }    
    iEqu>0

  }

  //1.按（QQ，TDID）等ID信息，连接同一用户的号码，并保存在 gEqu
  private def findEqualANPair() {
    //从每一个号码出发，遍历
    for (an <- ans) {
      //每号码的所有id关联(QQ,TDID,id)        
      val linkgroups = getIdLinks(an)

      //根据关联强度，关联同一用户的号码，        
      //如果优化，可以移出已经加过的? 不行可能共用同一号码.
      for (linkgroup <- linkgroups) {
        if (isSameUserNew(an, linkgroup._1, linkgroup._2)) {
          //是有向的所以只用加入一条边
          if (gEqu.contains(an)) {
            var cns = gEqu.get(an).get
            cns ++= List(linkgroup._1)
            gEqu -= an
            gEqu += (an -> cns)
          } else {
            gEqu += (an -> Set(linkgroup._1))
          }
        }
      }
    }
  }

  //找出从vSrc号码节点出发，中间通过id节点(QQ,TDID, ID等)，到号码节点的连接
  //是简化的2层dfs遍历
  private def getIdLinks(vSrc: String): Map[String, Set[String]] = {

    var idLnks = Map[String, Set[String]]()

    val lnksSrc = g.getOrElse(vSrc, Set[String]())
    for (vInter <- lnksSrc) {
      val lnksInter = g.getOrElse(vInter, Set[String]())
      for (vDst <- lnksInter) {
        if (vDst.compareToIgnoreCase(vSrc) == 0) {
          //到自己的连接忽略
        } else {
          val typ = vDst.substring(0, 2)

          //dst也是号码节点，则记录下此连接
          if (HGraphUtil.STR_ACCS_NUM.compareToIgnoreCase(typ) == 0 ||
            HGraphUtil.STR_MBL_NUM.compareToIgnoreCase(typ) == 0 ||
            HGraphUtil.STR_WB_NUM.compareToIgnoreCase(typ) == 0) {
            if (idLnks.contains(vDst)) {
              var idSet = idLnks.get(vDst).get
              idSet ++= List(vInter)
              idLnks -= vDst
              idLnks += (vDst -> idSet)
            } else {
              idLnks += (vDst -> Set(vInter))
            }
          }
        }
      }
    }
    //println("idLnks:  from " + vSrc + " to " + idLnks.toString())
    idLnks
  }

  //根据规则确定是否是同一用户的
  private def isSameUserNew(vSrc: String, vDst: String, idLnks: Set[String]) = {

    if (vSrc.substring(2).compareTo(vDst.substring(2)) == 0) { //固话号与宽带号相同的情景   
      true
    } else {
      var iQQ = 0
      var iTDID = 0
      var iID = 0
      var iCI = 0

      var iSrcHasQQ = hasQQNeighbor(vSrc)
      var iDstHasQQ = hasQQNeighbor(vDst)

      var sID = ""
      var sCust_ID = ""

      val styp = vSrc.substring(0, 2)
      val dtyp = vDst.substring(0, 2)

      //至少需要有一个移动号, 才能比较. 宽带号(可以属于多个用户)之间暂不考虑
      if (styp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0
        || dtyp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {

        for (vInter <- idLnks) {
          val typ = vInter.substring(0, 2)

          typ match {
            //移动号与移动号, 移动号与固话/宽带号之间可以通过QQ相等
            case HGraphUtil.STR_QQ => {
              iQQ = iQQ + 1
            }
            case "TDID" => {
              iTDID = iTDID + 1
            }
            case HGraphUtil.STR_ID_NUM => {
              //只有移动号与固话/宽带号之间可以通过ID相等
              if (dtyp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0
                || styp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0
                || dtyp.compareToIgnoreCase(HGraphUtil.STR_WB_NUM) == 0
                || styp.compareToIgnoreCase(HGraphUtil.STR_WB_NUM) == 0) {
                iID = iID + 1
                sID = vInter
              }
            }
            case HGraphUtil.STR_CUST_ID => {
              //只有移动号与固话/宽带号之间可以通过客户ID相等
              if (dtyp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0
                || styp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0
                || dtyp.compareToIgnoreCase(HGraphUtil.STR_WB_NUM) == 0
                || styp.compareToIgnoreCase(HGraphUtil.STR_WB_NUM) == 0) {
                iCI = iCI + 1
                sCust_ID = vInter
              }
            }
            case _ =>
          }
        }
      }

      var iweight = 0

      if (iQQ > 0) {
        //IF网络账号(QQ/TDID)相同
        //IF[
        //(客户ID相同 and (身份证相同 | 身份证号为空) and (移动接入号码非空))|
        //((客户ID不同 and 身份证号相同 and (移动接入号码不同 and 固网接入号码相同)) |
        //((客户ID不同 and 身份证号为空 and (移动接入号码不同 | (移动接入号码相同 and 固网接入号码不同))) |
        //( 客户ID不同 and 身份证号不同 and ((移动接入号码不同 and (固网接入号码不同 |固网接入号码空)) | (移动接入号码相同 and固网接入号码不同))) |
        //(客户ID不同 and身份证号不同 and ((移动号码不同) | (移动接入号码相同 and 固网接入号码不同))) |
        //(客户ID不同 and身份证号相同 and移动号码不同 and固网接入号码不同) |
        //[客户ID不同 and 身份证号相同 and移动接入号码不同 and 固网接入号码为空] |
        //(客户ID不同 and身份证号空 and ((移动接入号码相同 and 固网接入号码非空) | (移动接入号码不同 and (固网接入号码为空 | 固网接入号码不同))))
        //]
        iweight = 1
      } else {
        //没有网络帐号(QQ/TDID)
        //IF[(归属城市标识相同 and客户ID相同 and 身份证号码相同 and移动接入号码相同 and (固网接入号码相同 |固网接入号码空)) |
        //(归属城市标识相同 and客户ID相同 and 身份证号码空 and移动接入号码相同 and (固网接入号码相同 |固网接入号码空))
        //]
        //		生成唯一UID
        if (iSrcHasQQ < 1 && iDstHasQQ < 1) {
          if ((iCI == 1 && countMobileNeighbors(sCust_ID) == 1)
            && ((iID == 1 && countMobileNeighbors(sID) == 1) || (iID == 0))) {
            iweight = 1
          } else {
            if ((iCI == 0) && (iID == 1 && countMobileNeighbors(sID) == 1)) {
              iweight = 1
            }
          }
        }
      }

      iweight >= 1
    }

  }

  //是否有QQ邻居节点
  private def hasQQNeighbor(vSrc: String): Int = {
    var bHasQQ = 0
    val lnks = g.getOrElse(vSrc, Set[String]())
    for (vNeighbor <- lnks) {
      val typ = vNeighbor.substring(0, 2)
      if (typ.compareToIgnoreCase(HGraphUtil.STR_QQ) == 0) {
        bHasQQ = 1
      }
    }
    bHasQQ
  }

  //是否有多个移动号邻居节点
  private def countMobileNeighbors(vSrc: String): Int = {
    var iMobileNeighbors = 0
    val lnks = g.getOrElse(vSrc, Set[String]())
    for (vNeighbor <- lnks) {
      val typ = vNeighbor.substring(0, 2)
      if (typ.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {
        iMobileNeighbors += 1
      }
    }
    iMobileNeighbors
  }

  //2.把同一用户的号码放在同一组，也就是独立用户( 包括1个或多个号码级用户合并的结果)
  private def getANGroups(): List[Set[String]] = {
    var angroups = List[Set[String]]()
    var nodesVisitied = Set[String]()
    var nodesInHand = Set[String]()
    for (an <- ans) {
      //println("an is " + an)

      //      //只考虑含有移动号码的号码组
      //      val typ = an.substring(0, 2)
      //      if (typ.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {

      if (nodesVisitied.contains(an)) {
        //已经加入过  
      } else {
        nodesInHand += an
        dfsAnGroups(an, 2, nodesInHand) //目前规则来看2层就够了
        angroups = angroups ::: List(nodesInHand)

        for (v <- nodesInHand) {
          //            //移动号码是只用一次
          //            val vtyp = v.substring(0, 2)
          //            if (vtyp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {
          //              nodesVisitied += v
          //            }
          //移动号码是只用一次
          nodesVisitied += v
        }
        //nodesVisitied ++= nodesInHand
        nodesInHand = Set[String]()
      }
      //      }

    }
    angroups
  }

  //找到同一用户的号码的深度优先遍历
  private def dfsAnGroups(an: String, depth: Int, nodesInHand: Set[String]) {
    if (gEqu.contains(an)) {
      val connections = gEqu.get(an).get
      if (connections.size < 100) { //ID类节点不应该有太多连接
        for (con <- connections) {
          //println("con is " + con)
          // 是否新的节点
          if (nodesInHand.contains(con)) {
            //continue
          } else {

            nodesInHand += con

            val typ = con.substring(0, 2)
            // 按距离过滤。
            // 如果还没到底,继续深度优先遍历。
            if (depth > 1) {
              //只有移动号码可以传递相等关系
              if (typ.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {
                dfsAnGroups(con, depth - 1, nodesInHand);
              }
              //相等关系在之前已判断，可以通过固网号传递
              //dfsAnGroups(con, depth - 1, nodesInHand);
            }
          }
        }
      }
    }
  }

 

}