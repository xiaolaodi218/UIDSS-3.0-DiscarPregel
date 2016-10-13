package cn.ctyun.UIDSS.uidop

import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil }
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

object UIDGraph {
  private var printCount = 1
  def apply(group: List[(String, String)]) = {
    //println("********************  UIDGraph.apply ***************************")
    val graph = new UIDGraph(group)
    graph.init()
    graph
  }
  def getPrintCount() = {
    if (printCount > 0) {
      printCount = printCount - 1
      1
    } else {
      0
    }
  }
}

/*  用户ID生成
*/
class UIDGraph(group: List[(String, String)]) {
  //所有节点放到图(即邻接数组)中
  private var g = Map[String, Set[String]]()
  //所有移网号码,固网号码,宽带号码
  private var ans = List[String]()
  //找到的号码间相等关系图
  private var gEqu = Map[String, Set[String]]()

  def getSize(): Int = {
    g.size
  }

  def getBigestNode(): String = {
    var bn = ""
    var bs = 0
    for (nod <- g) {
      if (bs < nod._2.size) {
        bs = nod._2.size
        bn = nod._1
      }
    }
    bn
  }

  /*  init :  初始化图，并找出号码
 *  		1. 所有节点放到图(即邻接数组)中 g
 *  		2.以号码为中心，拆开连通图，形成号码级用户 ans
 *  		3.按（QQ，TDID）等ID信息连接号码级用户，并保存在 gEqu
*/
  private def init() = {
    var needPrint = false
    //println("********************  UIDGraph.init ***************************" + group.toString() )
    for (v <- group) {
      if ("QQ974834277".compareToIgnoreCase(v._1) == 0) {
        needPrint = true
      }
      //1.所有节点放到图(即邻接数组)中
      val fields = v._2.split(";")
      if (v._2.length() > 0 && fields.size > 0) {
        var neighbors = Set[String]()
        neighbors ++= fields.toList
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
  def getUID(): Iterable[(String, List[(String, String)])] = {

    //println("******************** UIDGraph.getUID ************************")

    //0.找出超级ID单独放在表中

    //1.按（QQ，TDID）等ID信息，连接同一用户的号码，并保存在 gEqu
    findEqualANPair()
    //println("findEqualANPair(), map gEqu: " + gEqu.toString())

    //2.把同一用户的号码放在同一组，也就是独立用户( 包括1个或多个号码级用户合并的结果)
    val angroups = getANGroups()
    //println("getANGroups(), angroups is : " + angroups.toString())

    //3.找出或生成用户号码组的UID（QQ，TDID>号码 > 新生成）
    var uidsUsed = Set[String]() //不同的组不能重复使用同一个UID；同时也可以分割原来相连的UID
    val buf = new ListBuffer[(String, List[(String, String)])]
    for (group <- angroups) {
      var vVisitied = Set[String]() //避免重复添加
      val lstEdgeWithUid = getPreferUID(group, vVisitied, uidsUsed)
      buf += lstEdgeWithUid
    }
    //返回UID，最后写回HBase
    buf.toIterable
  }

  //1.按（QQ，TDID）等ID信息，连接同一用户的号码，并保存在 gEqu
  private def findEqualANPair() {
    //从每一个号码出发，遍历
    //从手机号码出发
    for (an <- ans) {
      //每号码的所有id关联(QQ,TDID,id)        
      val linkgroups = getIdLinks(an)

      //根据关联强度，关联同一用户的号码，        
      //如果优化，可以移出已经加过的? 不行可能共用同一号码.
      for (linkgroup <- linkgroups) {
        if (isSameUser(an, linkgroup._1, linkgroup._2)) {
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
  private def isSameUser(vSrc: String, vDst: String, idLnks: Set[String]) = {
    var iQQ = 0;
    var iTDID = 0;
    var iID = 0;
    var iCI = 0;

    for (vInter <- idLnks) {
      val typ = vInter.substring(0, 2)
      val styp = vSrc.substring(0, 2)
      val dtyp = vDst.substring(0, 2)

      typ match {
        case HGraphUtil.STR_QQ => {
          iQQ = iQQ + 1
        }
        case "singleMNId" => {
          iTDID = iTDID + 1
        }
        case HGraphUtil.STR_ID_NUM => {
          //只有移动号与固话号之间可以通过ID相等
          if (styp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0 && dtyp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0) {
            iID = iID + 1
          }
        }
        case HGraphUtil.STR_CUST_ID => {
          //只有移动号与固话号之间可以通过ID相等
          if (styp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0 && dtyp.compareToIgnoreCase(HGraphUtil.STR_ACCS_NUM) == 0) {
            iCI = iCI + 1
          }
        }
        case _ =>
      }
    }

    var iweight = iQQ * 100 + iID + iCI + iTDID
    iweight >= 1
  }

  //2.把同一用户的号码放在同一组，也就是独立用户( 包括1个或多个号码级用户合并的结果)
  private def getANGroups(): List[Set[String]] = {
    var angroups = List[Set[String]]()
    var nodesVisitied = Set[String]()
    var nodesInHand = Set[String]()
    for (an <- ans) {
      //println("an is " + an)

      //只考虑含有移动号码的号码组
      val typ = an.substring(0, 2)
      if (typ.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {

        if (nodesVisitied.contains(an)) {
          //已经加入过  
        } else {
          nodesInHand += an
          dfsAnGroups(an, 2, nodesInHand) //目前规则来看2层就够了
          angroups = angroups ::: List(nodesInHand)

          for (v <- nodesInHand) {
            //移动号码是只用一次
            val vtyp = v.substring(0, 2)
            if (vtyp.compareToIgnoreCase(HGraphUtil.STR_MBL_NUM) == 0) {
              nodesVisitied += v
            }
          }
          //nodesVisitied ++= nodesInHand
          nodesInHand = Set[String]()
        }
      }

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
            }
          }
        }
      }
    }
  }

  /*3.找出或生成用户号码组的UID（QQ，TDID>号码 > 新生成）
   * @groupset	 用户的所有电话号码		
   * @return  返回 UID边列表 (新UID： String, List[(节点：String, 原UID： String)])，以后可以写回HBase
  */
  private def getPreferUID(groupset: Set[String], vVisitied: Set[String], uidsUsed: Set[String]): (String, List[(String, String)]) = {

    var needPrint = false
    //返回与号码组相邻的所有ID节点(也包括号码节点自身), 和对应UID。
    //是简化的2层dfs遍历
    val lstbufUIDs = new ListBuffer[(String, String)]
    for (an <- groupset) {
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
          //不是ID节点, 则进一步搜索连接的节点
          case HGraphUtil.STR_QQ => {
            val lnks2nd = g.getOrElse(vID, Set[String]())
            //与ID节点相邻的所有节点
            for (vIDLnk <- lnks2nd) {
              val typ2 = vIDLnk.substring(0, 2)
              typ2 match {
                //是UID节点, 则记为ID节点的UID
                case HGraphUtil.STR_UD => {
                  //ＱＱ节点连接的ＵＩＤ
                  if (!vVisitied.contains(vID)) {
                    vVisitied += vID
                    lstbufUIDs += ((vID, vIDLnk))
                  }
                }
                case _ =>
              }
            }
            //QQ如果没有关联的UID,则加入空. 因为所有标识节点都应该保存在lstbufUIDs,哪怕是空的.
            if (!vVisitied.contains(vID)) {
              vVisitied += vID
              lstbufUIDs += ((vID, ""))
            }
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
    val lstGroupUID: List[(String, String)] = lstbufUIDs.toList

    //算出每个UID的加权值
    var uidCounts = Map[String, Int]()
    for (vertex <- lstGroupUID) {
      //没有UID的节点(是为了以后补上UID加的)不参加优势UID
      if (vertex._2.length() > 0) {
        var count = uidCounts.getOrElse(vertex._2, 0)
        //与不同类型节点相连的UID权重是不同的
        val typ = vertex._1.substring(0, 2)
        typ match {
          case HGraphUtil.STR_QQ => {
            if ("QQ974834277".compareToIgnoreCase(vertex._1) == 0) {
              needPrint = true
            }
            count += GenUIDExt.UID_PRIOR_QQ
          }
          case HGraphUtil.STR_CUST_ID  => count += GenUIDExt.UID_PRIOR_CI
          case HGraphUtil.STR_ID_NUM   => count += GenUIDExt.UID_PRIOR_ID
          case HGraphUtil.STR_ACCS_NUM => count += GenUIDExt.UID_PRIOR_AN
          case HGraphUtil.STR_MBL_NUM  => count += GenUIDExt.UID_PRIOR_AN
          case HGraphUtil.STR_WB_NUM   => count += GenUIDExt.UID_PRIOR_AN
          case _                       => count = count + 1
        }
        uidCounts += (vertex._2 -> count)
      }
    }

    //出现次数最多的UID被选为有效UID
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

    //已经用过的UID不能再用
    if (maxUid.length() > 0 && uidsUsed.contains(maxUid)) {
      maxUid=""
    }    
    
    if (maxUid.length() == 0) {
      if (UIDGraph.getPrintCount() > 0) {
        needPrint = true
      }
      maxUid = HGraphUtil.STR_UD + generateUID()
    }
    
    uidsUsed += maxUid

    if (needPrint) {
      println("**********graph  is " + g.mkString("|"))
      println("**********all numbers: ans  is " + ans.mkString("|"))
      println("**********all equal numbers: gEqu  is " + gEqu.mkString("|"))
      println("**********current equal group: groupset  is " + groupset.mkString("|"))
      println("**********uid pairs in current group: lstGroupUID is " + lstGroupUID.mkString("|"))
      println("**********uid counts in current group: uidCounts is " + uidCounts.mkString("|"))
    }

    (maxUid, lstGroupUID)
  }

  //生成新的唯一的UID号
  private def generateUID(): String = {
    UUID.randomUUID().toString().replace("-", "")
  }

}